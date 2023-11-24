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
    // TODO: When all indexing columns are provided with a boundary, a new revision is
    //  created directly. If the actual data boundaries are not contained by those
    //  values, the RevisionID would then increase again by 1, leaving a discontinued
    //  sequence of RevisionIDs in the metadata.

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
  private[index] def computePartitionCubeDomains(
      numElements: Long,
      revision: Revision,
      indexStatus: IndexStatus,
      isReplication: Boolean): DataFrame => Dataset[CubeDomain] =
    (weightedDataFrame: DataFrame) => {
      val spark = SparkSession.active
      import spark.implicits._

      val indexColumns =
        if (isReplication) Seq(weightColumnName, cubeToReplicateColumnName)
        else Seq(weightColumnName)

      val cols = revision.columnTransformers.map(_.columnName) ++ indexColumns

      // Estimate the desiredSize of the cube at partition level
      val numPartitions: Int = weightedDataFrame.rdd.getNumPartitions
      val bufferCapacity: Long = CUBE_WEIGHTS_BUFFER_CAPACITY

      val selected = weightedDataFrame.select(cols.map(col): _*)
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
   * Aggregate partition cube domains to obtain global domains
   * @param partitionCubeDomains cubeBytes-domain pairs from all partitions
   * @param revision current index revision
   * @return A Map of domain value for all unique CubeIds
   */
  private[index] def estimateGlobalCubeDomains(
      partitionCubeDomains: Dataset[CubeDomain],
      revision: Revision): Dataset[(CubeId, Double)] = {
    val spark = SparkSession.active
    import spark.implicits._

    partitionCubeDomains
      .groupBy("cubeBytes")
      .agg(sum("domain"))
      .map { row =>
        val bytes = row.getAs[Array[Byte]](0)
        val domain = row.getAs[Double](1)
        (revision.createCubeId(bytes), domain)
      }
  }

  /**
   * Avoid computing the weight for the current cube if any of its ancestors is leaf.
   * @param cube the current CubeId whose NormalizedWeight is of our interest
   * @param cubeNormalizedWeights existing NormalizedWeights
   * @param isReplication whether the current process is a replication process
   * @return
   */
  private[index] def skipCube(
      cube: CubeId,
      cubeNormalizedWeights: Map[CubeId, NormalizedWeight],
      isReplication: Boolean): Boolean = {
    val minLeafWeight = 1d
    !isReplication && !cube.isRoot && (cubeNormalizedWeights
      .getOrElse(cube.parent.get, minLeafWeight) >= minLeafWeight)
  }

  /**
   * Populate global NormalizedWeights in a top-down fashion using cube domains:
   * Wc = Wpc + desiredCubeSize / domain. When domain smaller than or equal to
   * desiredCubeSize, we force the cube to be a leaf.
   * @param globalDomain cube domain from the entire dataset
   * @param indexStatus existing Cube NormalizedWeights
   * @param isReplication whether the current process is a replication process
   * @return
   */
  private[index] def estimateCubeWeights(
      globalDomain: Seq[(CubeId, Double)],
      indexStatus: IndexStatus,
      isReplication: Boolean): Map[CubeId, NormalizedWeight] = {
    var cubeNormalizedWeights = Map.empty[CubeId, NormalizedWeight]
    val desiredCubeSize = indexStatus.revision.desiredCubeSize

    val levelCubes = globalDomain.groupBy(_._1.depth)
    val (minLevel, maxLevel) = (levelCubes.keys.min, levelCubes.keys.max)

    (minLevel to maxLevel).foreach { level =>
      levelCubes(level)
        .filterNot(cd => skipCube(cd._1, cubeNormalizedWeights, isReplication))
        .foreach { case (cube, domain) =>
          val parentWeight = cube.parent match {
            case None => 0d
            case Some(parent) =>
              if (isReplication && !cubeNormalizedWeights.contains(parent)) {
                indexStatus.cubesStatuses(parent).normalizedWeight
              } else cubeNormalizedWeights(parent)
          }

          val treeSize = domain * (1d - parentWeight).max(1d)

          val normalizedWeight =
            if (treeSize <= desiredCubeSize && !isReplication) {
              NormalizedWeight(desiredCubeSize, treeSize.toLong)
            } else parentWeight + (desiredCubeSize / domain)

          cubeNormalizedWeights += (cube -> normalizedWeight)
        }
    }

    cubeNormalizedWeights
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
      if (isReplication) None // IF is replication
      else calculateRevisionChanges(dataFrameStats, indexStatus.revision)

    // The revision to use
    val revision = spaceChanges match {
      case Some(revisionChange) =>
        revisionChange.createNewRevision
      case None => indexStatus.revision
    }

    // First, add a random weight column
    val weightedDataFrame = dataFrame.transform(addRandomWeight(revision))

    // Second, compute partition-level cube domains
    val partitionCubeDomains: Dataset[CubeDomain] =
      weightedDataFrame
        .transform(computePartitionCubeDomains(numElements, revision, indexStatus, isReplication))

    // Estimate global domain
    val globalDomain: Seq[(CubeId, Double)] =
      estimateGlobalCubeDomains(partitionCubeDomains, revision).collect()

    // Populate NormalizedWeight level-wise from top to bottom
    val estimatedCubeWeights: Map[CubeId, NormalizedWeight] =
      estimateCubeWeights(globalDomain, indexStatus, isReplication)

    // Gather the new changes
    val tableChanges = BroadcastedTableChanges(
      spaceChanges,
      indexStatus,
      estimatedCubeWeights,
      globalDomain.toMap,
      if (isReplication) indexStatus.cubesToOptimize
      else Set.empty[CubeId])

    (weightedDataFrame, tableChanges)
  }

}
