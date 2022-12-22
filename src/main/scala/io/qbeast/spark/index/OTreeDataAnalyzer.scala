/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.core.transform.Transformer
import io.qbeast.spark.index.QbeastColumns.{cubeToReplicateColumnName, weightColumnName}
import io.qbeast.spark.internal.QbeastFunctions.qbeastHash
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Analyzes the data and extracts OTree structures
 */
trait OTreeDataAnalyzer {

  /**
   * Analyze the data to process
   * @param data the data to index
   * @param indexStatus the current status of the index
   * @param isReplication either we are replicating the elements or not
   * @return the changes to the index
   */
  def analyze(
      data: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges)

}

object DoublePassOTreeDataAnalyzer extends OTreeDataAnalyzer with Serializable {

  /**
   * Estimates MaxWeight on DataFrame
   */

  /**
   * Analyze a specific group of columns of the dataframe
   * and extract valuable statistics
   * @param data the data to analyze
   * @param columnTransformers the columns to analyze
   * @return
   */
  private[index] def getDataFrameStats(
      data: DataFrame,
      columnTransformers: IISeq[Transformer]): Row = {
    val columnStats = columnTransformers.map(_.stats)
    val columnsExpr = columnStats.flatMap(_.statsSqlPredicates)
    data.selectExpr(columnsExpr ++ Seq("count(1) AS count"): _*).first()
  }

  /**
   * Given a Row with Statistics, outputs the RevisionChange
   * @param row the row with statistics
   * @param revision the current revision
   * @return
   */
  private[index] def calculateRevisionChanges(
      row: Row,
      revision: Revision): Option[RevisionChange] = {

    val newTransformation =
      revision.columnTransformers.map(_.makeTransformation(colName => row.getAs[Object](colName)))

    val transformationDelta = if (revision.transformations.isEmpty) {
      newTransformation.map(a => Some(a))
    } else {
      revision.transformations.zip(newTransformation).map {
        case (oldTransformation, newTransformation)
            if oldTransformation.isSupersededBy(newTransformation) =>
          Some(oldTransformation.merge(newTransformation))
        case _ => None
      }
    }

    if (transformationDelta.flatten.isEmpty) {
      None
    } else {
      Some(
        RevisionChange(
          supersededRevision = revision,
          timestamp = System.currentTimeMillis(),
          transformationsChanges = transformationDelta))

    }

  }

  private[index] def addRandomWeight(revision: Revision): DataFrame => DataFrame =
    (df: DataFrame) => {
      df.withColumn(
        weightColumnName,
        qbeastHash(revision.columnTransformers.map(name => df(name.columnName)): _*))
    }

  /**
   * Extract data summaries by indexing each partition
   */
  private[index] def computeLocalTrees(
      numElements: Long,
      revision: Revision,
      indexStatus: IndexStatus,
      isReplication: Boolean): DataFrame => Dataset[LocalTree] =
    (weightedDataFrame: DataFrame) => {
      val spark = SparkSession.active
      import spark.implicits._

      val indexColumns = if (isReplication) {
        Seq(weightColumnName, cubeToReplicateColumnName)
      } else {
        Seq(weightColumnName)
      }
      val cols = revision.columnTransformers.map(_.columnName) ++ indexColumns

      // Estimate the desiredSize of the cube at partition level
      val numPartitions: Int = weightedDataFrame.rdd.getNumPartitions
      val bufferCapacity: Long = CUBE_WEIGHTS_BUFFER_CAPACITY

      val selected = weightedDataFrame
        .select(cols.map(col): _*)
      val weightIndex = selected.schema.fieldIndex(weightColumnName)

      selected
        .mapPartitions(rows => {
          val weights =
            new CubeWeightsBuilder(
              indexStatus = indexStatus,
              numPartitions = numPartitions,
              numElements = numElements,
              bufferCapacity = bufferCapacity)
          rows.foreach { row =>
            val point = RowUtils.rowValuesToPoint(row, revision)
            val weight = Weight(row.getAs[Int](weightIndex))
            if (isReplication) {
              val parentBytes = row.getAs[Array[Byte]](cubeToReplicateColumnName)
              val parent = Some(revision.createCubeId(parentBytes))
              weights.update(point, weight, parent)
            } else weights.update(point, weight)
          }
          weights.result().iterator
        })

    }

  /**
   * Extract tree size and domain from a given local tree.
   * The domain of a missing cube is computed from its immediate parent node,
   * in a top-down fashion.
   * @param cube CubeId whose tree size and domain are of our interest
   * @param localTree LocalTree from which to extract tree size and domain
   * @param treeSizeDomain existing tree sizes and domains
   * @param isReplication whether the current process is an optimization process
   * @return
   */
  private[index] def computeLocalTreeSizeAndDomain(
      cube: CubeId,
      localTree: LocalTree,
      treeSizeDomain: Map[CubeId, TreeSizeAndDomain],
      isReplication: Boolean): TreeSizeAndDomain = {
    localTree.get(cube) match {
      case Some(info) =>
        // Cube exists in the local tree, compute domain from tree size.
        TreeSizeAndDomain(info.treeSize, info.treeSize / (1d - info.parentWeight))
      case None =>
        // Cube missing from the local tree, compute domain from its parent domain.
        // Root should always exists in all local trees, and it's never present
        // during replication.
        val parent = cube.parent.get
        val (parentTreeSize, parentDomain) =
          if (isReplication && !treeSizeDomain.contains(parent)) {
            (0d, 0d)
          } else {
            val ptsd = treeSizeDomain(parent)
            (ptsd.treeSize, ptsd.domain)
          }
        val siblings = parent.children.filter(localTree.contains)
        val domain = if (siblings.isEmpty) {
          // Parent cube has no child cubes. Consider its domain is uniquely
          // composed of that of the missing cube.
          // Extract payload size from parent value to avoid overestimating missing cube domain.
          parentDomain - parentTreeSize
        } else {
          // Assume parent domain distribution follows that of the its child node tree sizes,
          // and the missing cube tree size is 1.
          val siblingSize = siblings.map(localTree(_).treeSize).sum
          val f = 1d / (siblingSize + 1d)
          f * parentDomain
        }
        TreeSizeAndDomain(0d, domain)
    }
  }

  /**
   * Compute cube and domain from all LocalTrees by broadcasting
   * the unique cubes from all partitions.
   * @param localTrees the collection of LocalTrees from all partitions
   * @param isReplication whether the current process is a replication process
   * @param dimensionCount the number of indexing columns
   * @param desiredCubeSize the desired cube size for the index
   * @return
   */
  private[index] def computeGlobalDomain(
      localTrees: Dataset[LocalTree],
      isReplication: Boolean,
      dimensionCount: Int,
      desiredCubeSize: Int): Seq[(CubeId, Double)] = {
    val spark = SparkSession.active
    import spark.implicits._

    // Unique cubes among all local trees
    val globalCubes = spark.sparkContext.broadcast(
      localTrees
        .select(explode(map_keys(col("value"))))
        .select(col("col.*"))
        .as[CubeId]
        .distinct()
        .rdd
        .sortBy(identity)
        .collect())

    // Compute and aggregate cube tree sizes and domains from all partitions
    val globalDomain = localTrees
      .flatMap(tree => {
        globalCubes.value
          .foldLeft(Map.empty[CubeId, TreeSizeAndDomain]) { (accSizeAndDomain, cube) =>
            val tsd = computeLocalTreeSizeAndDomain(cube, tree, accSizeAndDomain, isReplication)
            accSizeAndDomain + (cube -> tsd)
          }
          .map { case (cube, tsd) => (cube.bytes, tsd) }
      })
      .groupBy(col("_1"))
      .agg(sum(col("_2.treeSize")).as("treeSize"), sum(col("_2.domain")).as("domain"))
      .selectExpr("_1", s"CASE WHEN treeSize <= $desiredCubeSize THEN treeSize ELSE domain END")
      .map { row =>
        val bytes = row.getAs[Array[Byte]](0)
        val domain = row.getAs[Double](1)
        (CubeId(dimensionCount, bytes), domain)
      }
      .collect()

    globalCubes.destroy()

    globalDomain
  }

  /**
   * Avoid computing the weight for the current cube if any of its ancestors is leaf.
   * @param cube the current CubeId whose NormalizedWeight is of our interest
   * @param globalCubeNormalizedWeights existing NormalizedWeights
   * @param isReplication whether the current process is a replication process
   * @return
   */
  private[index] def skipCube(
      cube: CubeId,
      globalCubeNormalizedWeights: Map[CubeId, NormalizedWeight],
      isReplication: Boolean): Boolean = {
    val minLeafWeight = 1d
    !isReplication && !cube.isRoot && (globalCubeNormalizedWeights
      .getOrElse(cube.parent.get, minLeafWeight) >= minLeafWeight)
  }

  /**
   * Populate global NormalizedWeights in a top-down fashion using cube domains:
   * Wc = Wpc + desiredCubeSize / domain. When domain is <= desiredCubeSize, we
   * force the cube to be a leaf.
   * @param globalDomain cube domain from the entire dataset
   * @param indexStatus existing Cube NormalizedWeights
   * @param isReplication whether the current process is a replication process
   * @return
   */
  private[index] def estimateCubeWeights(
      globalDomain: Seq[(CubeId, Double)],
      indexStatus: IndexStatus,
      isReplication: Boolean): Map[CubeId, NormalizedWeight] = {
    var globalCubeNormalizedWeights = Map.empty[CubeId, NormalizedWeight]
    val desiredCubeSize = indexStatus.revision.desiredCubeSize
    val levelCubes = globalDomain.groupBy(_._1.depth)
    val (minLevel, maxLevel) = (levelCubes.keys.min, levelCubes.keys.max)

    (minLevel to maxLevel).foreach(level => {
      levelCubes(level)
        .filterNot(cd => skipCube(cd._1, globalCubeNormalizedWeights, isReplication))
        .foreach { case (cube, domain) =>
          val normalizedWeight =
            if (!isReplication && domain <= desiredCubeSize) {
              NormalizedWeight.apply(desiredCubeSize, domain.toLong)
            } else {
              val parentGlobalWeight = cube.parent match {
                case None => 0d
                case Some(parent) =>
                  if (isReplication && !globalCubeNormalizedWeights.contains(parent)) {
                    indexStatus.cubesStatuses(parent).normalizedWeight
                  } else {
                    globalCubeNormalizedWeights(parent)
                  }
              }
              parentGlobalWeight + (desiredCubeSize / domain)
            }

          globalCubeNormalizedWeights += (cube -> normalizedWeight)
        }
    })

    globalCubeNormalizedWeights
  }

  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {

    // Compute the statistics for the indexedColumns
    val columnTransformers = indexStatus.revision.columnTransformers
    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)

    val numElements = dataFrameStats.getAs[Long]("count")
    if (numElements == 0) {
      throw new RuntimeException(
        "The DataFrame is empty, why are you trying to index an empty dataset?")
    }

    val spaceChanges =
      if (isReplication) None
      else calculateRevisionChanges(dataFrameStats, indexStatus.revision)

    // The revision to use
    val revision = spaceChanges match {
      case Some(revisionChange) =>
        revisionChange.createNewRevision
      case None => indexStatus.revision
    }

    // First, add a random weight column
    val weightedDataFrame = dataFrame.transform(addRandomWeight(revision))

    // Second, construct local trees with partition data
    val localTrees: Dataset[LocalTree] =
      weightedDataFrame
        .transform(computeLocalTrees(numElements, revision, indexStatus, isReplication))

    // Compute domain(upper-bound) for all unique cubes
    val globalDomain: Seq[(CubeId, Double)] =
      computeGlobalDomain(
        localTrees,
        isReplication,
        columnTransformers.size,
        revision.desiredCubeSize)

    // Populate NormalizedWeight level-wise from top to bottom
    val estimatedCubeWeights: Map[CubeId, NormalizedWeight] =
      estimateCubeWeights(globalDomain, indexStatus, isReplication)

    // Gather the new changes
    val tableChanges = BroadcastedTableChanges(
      spaceChanges,
      indexStatus,
      estimatedCubeWeights,
      if (isReplication) indexStatus.cubesToOptimize
      else Set.empty[CubeId])

    (weightedDataFrame, tableChanges)
  }

}
