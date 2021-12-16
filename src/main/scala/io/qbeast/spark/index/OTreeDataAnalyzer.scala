/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import com.typesafe.config.ConfigFactory
import io.qbeast.context.QbeastContext
import io.qbeast.core.model._
import io.qbeast.core.transform.ColumnStats
import io.qbeast.spark.index.QbeastColumns.{cubeToReplicateColumnName, weightColumnName}
import io.qbeast.spark.internal.QbeastFunctions.qbeastHash
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udaf}
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
  private val maxWeightEstimation: UserDefinedFunction = udaf(MaxWeightEstimation)

  /**
   * The minimum cube size per partition registered in configuration
   */
  private val minPartitionCubeSize: Int =
    ConfigFactory.load().getInt("qbeast.index.minPartitionCubeSize")

  private lazy val logger = org.apache.log4j.LogManager.getLogger(this.getClass)

  private[index] def calculateRevisionChanges(
      dataFrameStats: Array[Row],
      revision: Revision): Option[RevisionChange] = {

    val columnNames = revision.columnTransformers.map(_.columnName)

    val mins = dataFrameStats.filter(_.get(0) == "min").head.getValuesMap(columnNames)
    val maxs = dataFrameStats.filter(_.get(0) == "max").head.getValuesMap(columnNames)

    val newTransformation = revision.columnTransformers.map(ct => {
      val minVal: String = mins(ct.columnName)
      val maxVal: String = maxs(ct.columnName)
      val columnStats: ColumnStats = {
        ct.dataType match {
          case DoubleDataType => ColumnStats(minVal.toDouble, maxVal.toDouble, 0, 0.0, Nil)
          case IntegerDataType => ColumnStats(minVal.toInt, maxVal.toInt, 0, 0.0, Nil)
          case LongDataType => ColumnStats(minVal.toLong, maxVal.toLong, 0, 0.0, Nil)
          case FloatDataType => ColumnStats(minVal.toFloat, maxVal.toFloat, 0, 0.0, Nil)
          case DecimalDataType => ColumnStats(minVal.toDouble, maxVal.toDouble, 0, 0.0, Nil)
          case StringDataType => ColumnStats(Nil, Nil, 0, 0.0, Nil)
        }
      }
      ct.makeTransformation(columnStats)
    })

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
        .agg(maxWeightEstimation(col("normalizedWeight")))
        .map { row =>
          val bytes = row.getAs[Array[Byte]](0)
          val estimatedWeight = row.getAs[Double](1)
          (revision.createCubeId(bytes), estimatedWeight)
        }
    }

  private[index] def estimateGroupCubeSize(
      desiredCubeSize: Int,
      numPartitions: Int,
      numElements: Long,
      maxWorkingSetSize: Long): Double = {
    val numGroups = Math.max(numPartitions, numElements / maxWorkingSetSize)
    if (numGroups > 0) {
      val groupCubeSize =
        Math.ceil(desiredCubeSize.toDouble / numGroups + 1)
      if (groupCubeSize < minPartitionCubeSize) {
        logger.warn(
          s"Cube size per partition is less than $minPartitionCubeSize," +
            s" Set a bigger cubeSize before writing")
        minPartitionCubeSize
      } else groupCubeSize
    } else {
      // TODO should fail if the desiredCubeSize is < than minPartitionCubeSize?
      Math.max(desiredCubeSize, minPartitionCubeSize)
    }

  }

  private[index] def estimatePartitionCubeWeights(
      revision: Revision,
      indexStatus: IndexStatus,
      isReplication: Boolean,
      stats: Array[Row]): DataFrame => Dataset[CubeNormalizedWeight] =
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
      // If the user has specified a desiredSize too small
      // set it to minCubeSize
      val numPartitions: Int = weightedDataFrame.rdd.getNumPartitions
      val desiredCubeSize: Int = indexStatus.revision.desiredCubeSize
      val estimatedGroupCubeSize =
        estimateGroupCubeSize(
          desiredCubeSize = desiredCubeSize,
          numPartitions = numPartitions,
          numElements = stats.filter(_.get(0) == "count").head.getAs[String](1).toLong,
          maxWorkingSetSize =
            QbeastContext.config.getLong("qbeast.index.cubeWeightsBufferCapacity"))

      val selected = weightedDataFrame
        .select(cols.map(col): _*)
      val weightIndex = selected.schema.fieldIndex(weightColumnName)
      val cubeWeightsBufferCapacity =
        QbeastContext.config.getLong("qbeast.index.cubeWeightsBufferCapacity")
      selected
        .mapPartitions(rows => {
          val weights =
            new CubeWeightsBuilder(
              indexStatus = indexStatus,
              boostSize = estimatedGroupCubeSize,
              cubeWeightsBufferCapacity)
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

  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {

    // Compute the statistics for the indexedColumns
    val stats = dataFrame
      .describe(indexStatus.revision.columnTransformers.map(_.columnName): _*)
      .collect()

    // Check if the DataFrame is empty
    if (stats.filter(_.get(0) == "count").head.get(1) == "0") {
      throw new RuntimeException(
        "The DataFrame is empty. Are you trying to index an empty dataset?")
    }

    val spaceChanges =
      if (isReplication) None
      else calculateRevisionChanges(stats, indexStatus.revision)

    // The revision to use
    val revision = spaceChanges match {
      case Some(revisionChange) =>
        revisionChange.newRevision
      case None => indexStatus.revision
    }

    // Three step transformation

    // First, add a random weight column
    val weightedDataFrame = dataFrame.transform(addRandomWeight(revision))

    // Second, estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights = weightedDataFrame.transform(
      estimatePartitionCubeWeights(revision, indexStatus, isReplication, stats))

    // Third, compute the overall estimated cube weights
    val estimatedCubeWeights =
      partitionedEstimatedCubeWeights
        .transform(estimateCubeWeights(revision))
        .collect()
        .toMap

    // Gather the new changes
    val tableChanges = TableChanges(
      spaceChanges,
      IndexStatusChange(
        indexStatus,
        estimatedCubeWeights,
        deltaReplicatedSet =
          if (isReplication) indexStatus.cubesToOptimize
          else Set.empty[CubeId]))

    (weightedDataFrame, tableChanges)
  }

}
