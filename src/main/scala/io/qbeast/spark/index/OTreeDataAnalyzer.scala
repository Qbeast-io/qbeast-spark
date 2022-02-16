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

  private[index] def estimateCubeWeights(
      revision: Revision): Dataset[CubeNormalizedWeight] => Dataset[(CubeId, NormalizedWeight)] =
    (partitionedEstimatedCubeWeights: Dataset[CubeNormalizedWeight]) => {

      val sqlContext = SparkSession.active.sqlContext
      import sqlContext.implicits._

      // These column names are the ones specified in case class CubeNormalizedWeight
      partitionedEstimatedCubeWeights
        .groupBy("cubeBytes")
        .agg(lit(1) / sum(lit(1.0) / col("normalizedWeight")))
        .map { row =>
          val bytes = row.getAs[Array[Byte]](0)
          val estimatedWeight = row.getAs[Double](1)
          (revision.createCubeId(bytes), estimatedWeight)
        }
    }

  private[index] def estimatePartitionCubeWeights(
      numElements: Long,
      revision: Revision,
      indexStatus: IndexStatus,
      isReplication: Boolean): DataFrame => Dataset[CubeNormalizedWeight] =
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

      // Define a container for Min/Max values for each column
      // These values for each column have to be specific for each column's dataType

      // revision.columnTransformers.map(
      // _.makeTransformation(colName => row.getAs[Object](colName)))
      // var transformers
      // var globalTransformations

      selected
        .mapPartitions(rows => {
          // TODO:
          // Think of a way to iterate twice over the Iterator
          // One solution can be making a duplication:
          // val (rows1, rows2) = rows.duplicate

          // Initialize the min/max values on a partition level and
          // iterate through the iterator to update both the local and
          // global min/max's for each column

          // Once done, initialize the transformations that are to be used
          // in the second iteration to map rows to points

          // Find a way to store and return the partition level min/max and global
          // min/max. Both are used to do the CubeId mapping from local index to
          // global index.
          val (iterForStats, iterForCubeWeights) = rows.duplicate

          iterForStats.foreach { row =>
            // extract min/max for the columns to index => columnStats
            // globalTransformations.update(columnStata)

          }
          val weights =
            new CubeWeightsBuilder(
              indexStatus = indexStatus,
              numPartitions = numPartitions,
              numElements = numElements,
              bufferCapacity = bufferCapacity)
          iterForCubeWeights.foreach { row =>
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

  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {

    // Compute the statistics for the indexedColumns
    val columnTransformers = indexStatus.revision.columnTransformers
    val dataFrameStats = getDataFrameStats(dataFrame, columnTransformers)

//    val numElements = dataFrameStats.getAs[Long]("count")
    if (dataFrame.take(1).isEmpty) {
      throw new RuntimeException(
        "The DataFrame is empty, why are you trying to index an empty dataset?")
    }

    val spaceChanges =
      if (isReplication) None
      else calculateRevisionChanges(dataFrameStats, indexStatus.revision)

//     The revision to use
//    val revision = spaceChanges match {
//      case Some(revisionChange) =>
//        revisionChange.createNewRevision
//      case None => indexStatus.revision
//    }

    // Three step transformation

    // First, add a random weight column
    val weightedDataFrame =
      dataFrame.transform(addRandomWeight(indexStatus.revision))

    // Second, estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights = weightedDataFrame.transform(
      estimatePartitionCubeWeights(0, indexStatus.revision, indexStatus, isReplication))

    // Third, compute the overall estimated cube weights
    val estimatedCubeWeights =
      partitionedEstimatedCubeWeights
        .transform(estimateCubeWeights(indexStatus.revision))
        .collect()
        .toMap

    // Gather the new changes
    val tableChanges = TableChanges(
      None,
      IndexStatusChange(
        indexStatus,
        estimatedCubeWeights,
        deltaReplicatedSet =
          if (isReplication) indexStatus.cubesToOptimize
          else Set.empty[CubeId]))

    (weightedDataFrame, tableChanges)
  }

}

//case class columnStats()
