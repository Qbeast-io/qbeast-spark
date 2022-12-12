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

  // DATAFRAME TRANSFORMATIONS //

  private[index] def addRandomWeight(revision: Revision): DataFrame => DataFrame =
    (df: DataFrame) => {
      df.withColumn(
        weightColumnName,
        qbeastHash(revision.columnTransformers.map(name => df(name.columnName)): _*))
    }

  /**
   * Extract data summaries from the data by building Otree indexes on each partition
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

  private[index] def skipCube(
      cube: CubeId,
      globalCubeNormalizedWeights: Map[CubeId, NormalizedWeight],
      isReplication: Boolean): Boolean = {
    !isReplication && !cube.isRoot && (globalCubeNormalizedWeights
      .getOrElse(cube.parent.get, 1.0) >= 1.0)
  }

  private[index] def computeTreeSizeAndDomain(
      localTrees: Dataset[LocalTree],
      dimensionCount: Int,
      missingCubeDomain: (CubeId, LocalTree) => Double): Seq[TreeSizeAndDomain] = {
    val spark = SparkSession.active
    import spark.implicits._

    val globalCubes = spark.sparkContext.broadcast(
      localTrees
        .select(explode(map_keys(col("value"))))
        .select(col("col.*"))
        .as[CubeId]
        .distinct()
        .collect()
        .sorted)

    val globalTreeSizeAndDomain = localTrees
      .flatMap(tree => {
        var treeSizeDomain = Map.empty[CubeId, TsAndDomain]

        globalCubes.value.foreach(cube =>
          tree.get(cube) match {
            case Some(info) =>
              val treeSize = info.treeSize
              val domain = info.treeSize / (1d - info.parentWeight)
              treeSizeDomain += (cube -> TsAndDomain(treeSize, domain))
            case None =>
              val parent = cube.parent.get
              val parentTreeSize = treeSizeDomain(parent).treeSize
              val parentDomain = treeSizeDomain(parent).domain
              val siblings = parent.children.filter(tree.contains)
              val domain = if (siblings.isEmpty) {
                parentDomain - parentTreeSize
              } else {
                val siblingSize = siblings.map(tree(_).treeSize).sum
                val f = 1d / (siblingSize + 1d)
                f * parentDomain
              }

              treeSizeDomain += (cube -> TsAndDomain(0d, domain))
          })
        treeSizeDomain.toSeq.map { case (c, tsd) => (c.bytes, tsd.treeSize, tsd.domain) }
      })
      .groupBy(col("_1"))
      .agg(sum(col("_2")), sum(col("_3")))
      .map { row =>
        val bytes = row.getAs[Array[Byte]](0)
        val treeSize = row.getAs[Double](1)
        val domain = row.getAs[Double](2)
        TreeSizeAndDomain(CubeId(dimensionCount, bytes), treeSize, domain)
      }
      .collect()

    globalCubes.destroy()

    globalTreeSizeAndDomain
  }

  private[index] def estimateCubeWeights(
      globalTreeSizeAndDomains: Seq[TreeSizeAndDomain],
      indexStatus: IndexStatus,
      isReplication: Boolean): Map[CubeId, NormalizedWeight] = {
    var globalCubeNormalizedWeights = Map.empty[CubeId, NormalizedWeight]
    val desiredCubeSize = indexStatus.revision.desiredCubeSize
    val levelCubes = globalTreeSizeAndDomains.groupBy(_.cube.depth)
    val (minLevel, maxLevel) = (levelCubes.keys.min, levelCubes.keys.max)

    (minLevel to maxLevel).foreach(level => {
      levelCubes(level)
        .filterNot(ctd => skipCube(ctd.cube, globalCubeNormalizedWeights, isReplication))
        .foreach { case TreeSizeAndDomain(cube: CubeId, treeSize: Double, domain: Double) =>
          val parentGlobalWeight = cube.parent match {
            case None => 0d
            case Some(parent) =>
              if (isReplication && !globalCubeNormalizedWeights.contains(parent)) {
                indexStatus.cubesStatuses(parent).normalizedWeight
              } else {
                globalCubeNormalizedWeights(parent)
              }
          }
          val normalizedWeight =
            if (!isReplication && treeSize <= desiredCubeSize) {
              NormalizedWeight.apply(desiredCubeSize, treeSize.toLong)
            } else parentGlobalWeight + (desiredCubeSize / domain)

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

    // Compute tree size and domain for all unique cubes
    val treeSizeAndDomain =
      computeTreeSizeAndDomain(
        localTrees,
        columnTransformers.size,
        MissingCubeDomainEstimation.domainThroughPayloadFractions)

    // Populate NormalizedWeight level-wise from top to bottom
    val estimatedCubeWeights: Map[CubeId, NormalizedWeight] =
      estimateCubeWeights(treeSizeAndDomain, indexStatus, isReplication)

    // Gather the new changes
    val tableChanges = BroadcastedTableChanges(
      spaceChanges,
      indexStatus,
      estimatedCubeWeights,
      if (isReplication) indexStatus.cubesToOptimize
      else Set.empty[CubeId])

    (weightedDataFrame, tableChanges)
  }

  case class TsAndDomain(treeSize: Double, domain: Double)
}
