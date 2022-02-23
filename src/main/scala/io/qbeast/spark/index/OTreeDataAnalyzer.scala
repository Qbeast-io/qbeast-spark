/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.core.transform.{
  HashTransformation,
  LinearTransformation,
  Transformation,
  Transformer
}
import io.qbeast.spark.index.QbeastColumns.{cubeToReplicateColumnName, weightColumnName}
import io.qbeast.spark.internal.QbeastFunctions.qbeastHash
import org.apache.spark.qbeast.config.CUBE_WEIGHTS_BUFFER_CAPACITY
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

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
      isReplication: Boolean): DataFrame => Dataset[CubeWeightAndStats] =
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

      val statsExpr = revision.columnTransformers.flatMap(_.stats.statsSqlPredicates)

      val columnsToIndex = revision.columnTransformers.map(_.columnName)

      selected
        .mapPartitions(rows => {
          val (iterForStats, iterForCubeWeights) = rows.duplicate

          val partitionStats =
            spark
              .createDataFrame(iterForStats.toList.asJava, selected.schema)
              .selectExpr(statsExpr: _*)
              .first()

          val allColStats: Seq[ColStats] = columnsToIndex.map(name =>
            SparkRevisionFactory.getColumnQType(name, selected.schema) match {
              case dType: OrderedDataType => NumericColumnStats(name, dType)
              case dType => StringColumnStats(name, dType)
            })

          allColStats.foreach(colStats => colStats.update(partitionStats))

          val transformations: Seq[Transformation] = allColStats.map {
            case stats: NumericColumnStats =>
              LinearTransformation(stats.min, stats.max, stats.dType)
            case StringColumnStats(_, _) => HashTransformation()
          }

          val rev = revision.copy(transformations = transformations.toIndexedSeq)

          val weights =
            new CubeWeightsBuilder(
              indexStatus = indexStatus,
              numPartitions = numPartitions,
              numElements = numElements,
              bufferCapacity = bufferCapacity)

          iterForCubeWeights.foreach { row =>
            val point = RowUtils.rowValuesToPoint(row, rev)
            val weight = Weight(row.getAs[Int](weightIndex))
            if (isReplication) {
              val parentBytes = row.getAs[Array[Byte]](cubeToReplicateColumnName)
              val parent = Some(revision.createCubeId(parentBytes))
              weights.update(point, weight, parent)
            } else weights.update(point, weight)
          }
          weights
            .result()
            .map { case CubeNormalizedWeight(cubeBytes, weight) =>
              CubeWeightAndStats(cubeBytes, weight, allColStats)
            }
            .iterator
        })
    }

  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges) = {
    if (dataFrame.take(1).isEmpty) {
      throw new RuntimeException(
        "The DataFrame is empty, why are you trying to index an empty dataset?")
    }

    // First, add a random weight column
    val weightedDataFrame =
      dataFrame.transform(addRandomWeight(indexStatus.revision))

    // Second, estimate the cube weights at partition level
    val partitionedEstimatedCubeWeights = weightedDataFrame.transform(
      estimatePartitionCubeWeights(0, indexStatus.revision, indexStatus, isReplication))

    // Initialize and update global column statistics
    val globalColStats: Seq[ColStats] = initializeColStats(
      indexStatus.revision.columnTransformers.map(_.columnName),
      weightedDataFrame.schema)
    val partitionColumnStats = partitionedEstimatedCubeWeights.collect().map(_.colStats).toSet
    partitionColumnStats.foreach { partitionCol =>
      globalColStats zip partitionCol foreach (tup => tup._1.merge(tup._2))
    }

    val dimensionCount = indexStatus.revision.transformations.size
    val spark = SparkSession.active
    import spark.implicits._

    val adjustedCubeWeights = partitionedEstimatedCubeWeights
      .repartition(col("colStats"))
      .mapPartitions { iter =>
        val partitionCubeWeights = mutable.ArrayBuffer[CubeNormalizedWeight]()
        iter.foreach {
          case CubeWeightAndStats(
                cubeBytes: Array[Byte],
                normalizedWeight: NormalizedWeight,
                colStats: Seq[ColStats]) =>
            val cube = CubeId(dimensionCount, cubeBytes)

            val cubePartitionCoordinates = cube.from.coordinates.zip(cube.to.coordinates)
            val cubeVolume =
              cubePartitionCoordinates.foldLeft(1.0)((vol, point) => vol * (point._2 - point._1))

            val cubeGlobalCoordinates = {
              cubePartitionCoordinates zip (colStats zip globalColStats) map {
                case ((l: Double, r: Double), (_: StringColumnStats, _: StringColumnStats)) =>
                  (l, r)
                case (
                      (l: Double, r: Double),
                      (
                        partitionColStats: NumericColumnStats,
                        globalColStats: NumericColumnStats)) =>
                  (
                    toGlobalCoordinates(l, partitionColStats, globalColStats),
                    toGlobalCoordinates(r, partitionColStats, globalColStats))
              }
            }

            var cubeCandidates = Seq(CubeId.root(dimensionCount))
            (0 until dimensionCount).foreach { _ =>
              cubeCandidates = cubeCandidates.flatMap(c => c.children)
            }

            cubeCandidates.foreach { candidate =>
              val candidateCoordinates = candidate.from.coordinates zip candidate.to.coordinates
              val dimensionOverlaps = candidateCoordinates zip cubeGlobalCoordinates map {
                case (candidate, cube) =>
                  if (candidate._1 < cube._2 && cube._1 < candidate._2) {
                    (candidate._2 - cube._1).min(cube._2 - candidate._1)
                  } else 0.0
              }
              val candidateOverlap = dimensionOverlaps
                .foldLeft(1.0 / cubeVolume)((acc, overlap) => acc * overlap)
              if (candidateOverlap != 0.0) {
                partitionCubeWeights :+ CubeNormalizedWeight(
                  cubeBytes,
                  normalizedWeight / candidateOverlap)
              }
            }
        }
        partitionCubeWeights.toIterator
      }

    // Third, compute the overall estimated cube weights
    val estimatedCubeWeights =
      adjustedCubeWeights
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

  def toGlobalCoordinates(
      value: Double,
      partitionStats: NumericColumnStats,
      globalStats: NumericColumnStats): Double = {
    val partitionValue = value * partitionStats.scale + partitionStats.min
    (partitionValue - globalStats.min) / globalStats.scale
  }

  def initializeColStats(columnsToIndex: Seq[String], schema: StructType): Seq[ColStats] = {
    columnsToIndex.map(name =>
      SparkRevisionFactory.getColumnQType(name, schema) match {
        case dType: OrderedDataType => NumericColumnStats(name, dType)
        case dType => StringColumnStats(name, dType)
      })
  }

}

trait ColStats {
  def update(row: Row): Unit
  def merge(that: ColStats): Unit
}

case class NumericColumnStats(colName: String, dType: OrderedDataType) extends ColStats {
  private val minValueName = s"{colName}_min"
  private val maxValueName = s"{colName}_max"

  var (min, max) = (Double.MaxValue, Double.MinValue)
  def scale: Double = max - min

  override def update(row: Row): Unit = {
    min = min.min(row.getAs[Long](minValueName))
    max = max.max(row.getAs[Long](maxValueName))
  }

  override def merge(colStats: ColStats): Unit = colStats match {
    case numeric: NumericColumnStats
        if minValueName == numeric.minValueName && maxValueName == numeric.maxValueName =>
      min = min.min(numeric.min)
      max = max.max(numeric.max)
  }

}

case class StringColumnStats(colName: String, dType: QDataType) extends ColStats {
  var (min, max) = (Double.MinValue, Double.MaxValue)

  override def update(row: Row): Unit = {}

  override def merge(that: ColStats): Unit = {}
}

case class CubeWeightAndStats(
    cubeBytes: Array[Byte],
    normalizedWeight: NormalizedWeight,
    colStats: Seq[ColStats])
