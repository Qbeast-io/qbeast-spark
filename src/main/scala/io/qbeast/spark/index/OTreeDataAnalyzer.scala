/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import com.typesafe.config.ConfigFactory
import io.qbeast.model._
import io.qbeast.spark.index.QbeastColumns.{cubeToReplicateColumnName, weightColumnName}
import io.qbeast.spark.internal.QbeastFunctions.qbeastHash
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udaf}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
      data: DataFrame,
      revision: Revision): Option[RevisionChange] = {
    val columnStats = revision.columnTransformers.map(_.stats)
    val columnsExpr = columnStats.flatMap(_.columns)
    // This is a actions that will be executed on the dataframe
    val rows = data.selectExpr(columnsExpr: _*).collect()
    if (rows.isEmpty) {
      throw new RuntimeException(
        "The DataFrame is empty, why are you trying ot index an empty dataset?")
    }
    val row = rows.head
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
        .agg(maxWeightEstimation(col("normalizedWeight")))
        .map { row =>
          val bytes = row.getAs[Array[Byte]](0)
          val estimatedWeight = row.getAs[Double](1)
          (revision.createCubeId(bytes), estimatedWeight)
        }
    }

  // TODO add more logic and set at configurable parameter
  private[index] def estimatePartitionCubeSize(
      desiredCubeSize: Int,
      numPartitions: Int): Double = {
    if (numPartitions > 0) {
      val desiredPartitionCubeSize = desiredCubeSize.toDouble / numPartitions
      if (desiredPartitionCubeSize < minPartitionCubeSize) {
        logger.warn(
          s"Cube size per partition is less than $minPartitionCubeSize," +
            s" set a bigger cubeSize before writing")
        desiredCubeSize
      } else {
        desiredPartitionCubeSize
      }
    } else {
      desiredCubeSize
    }

  }

  private[index] def estimatePartitionCubeWeights(
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
      // If the user has specified a desiredSize too small
      // set it to minCubeSize
      val numPartitions: Int = weightedDataFrame.rdd.getNumPartitions
      val desiredCubeSize: Int = indexStatus.revision.desiredCubeSize
      val desiredPartitionCubeSize =
        estimatePartitionCubeSize(desiredCubeSize, numPartitions)

      weightedDataFrame
        .select(cols.map(col): _*)
        .mapPartitions(rows => {
          val weights =
            new CubeWeightsBuilder(
              desiredCubeSize = desiredCubeSize,
              boostSize = desiredPartitionCubeSize,
              indexStatus.announcedSet,
              indexStatus.replicatedSet)
          rows.foreach { row =>
            val point = RowUtils.rowValuesToPoint(row, revision)
            val weight = Weight(row.getAs[Int](weightColumnName))
            if (isReplication) {
              val parentBytes = row.getAs[Array[Byte]](cubeToReplicateColumnName)
              val parent = Some(revision.createCubeId(parentBytes))
              weights.update(point, weight, parent)
            } else weights.update(point, weight)
          }
          weights.result().iterator
        })
    }

  // ANALYZE METHODS
  /**
   * @param dataFrame
   * @param indexStatus
   * @param isReplication
   * @return
   */
  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {

    val spaceChanges =
      if (isReplication) None
      else calculateRevisionChanges(dataFrame, indexStatus.revision)

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
      estimatePartitionCubeWeights(revision, indexStatus, isReplication))

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
