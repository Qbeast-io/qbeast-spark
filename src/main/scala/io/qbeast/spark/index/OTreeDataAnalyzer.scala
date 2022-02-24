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
      isReplication: Boolean): (DataFrame, TableChanges, IISeq[Transformation])

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

      val columnsToIndex = revision.columnTransformers.map(_.columnName)
      var partitionColStats = initializeColStats(columnsToIndex, selected.schema)

      selected
        .mapPartitions(rows => {
          val (iterForStats, iterForCubeWeights) = rows.duplicate

          iterForStats.foreach { row =>
            partitionColStats = partitionColStats.map { stats =>
              if (stats.dType == "StringDataType") {
                stats
              } else {
                val value = stats match {
                  case ColStats(colName, "DoubleDataType", _, _) =>
                    row.getAs[Double](colName)
                  case ColStats(colName, "IntegerDataType", _, _) =>
                    row.getAs[Int](colName).toDouble
                  case ColStats(colName, "FloatDataType", _, _) =>
                    row.getAs[Float](colName).toDouble
                  case ColStats(colName, "DecimalDataType", _, _) =>
                    row.getAs[Double](colName)
                  case ColStats(colName, "LongDataType", _, _) =>
                    row.getAs[Long](colName).toDouble
                  case _ => throw new RuntimeException(s"Type currently not supported")
                }
                stats.copy(min = stats.min.min(value), max = stats.max.max(value))
              }
            }
          }

          val weights =
            new CubeWeightsBuilder(
              indexStatus = indexStatus,
              numPartitions = numPartitions,
              numElements = numElements,
              bufferCapacity = bufferCapacity)

          val partitionRevision =
            revision.copy(transformations = updatedTransformations(partitionColStats))
          iterForCubeWeights.foreach { row =>
            val point = RowUtils.rowValuesToPoint(row, partitionRevision)
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
              CubeWeightAndStats(cubeBytes, weight, partitionColStats)
            }
            .iterator
        })
    }

  private[index] def toGlobalCubeWeights(
      partitionedEstimatedCubeWeights: Dataset[CubeWeightAndStats],
      revision: Revision,
      schema: StructType): (Dataset[CubeNormalizedWeight], IISeq[Transformation]) = {
    val partitionColumnStats: Set[Seq[ColStats]] =
      partitionedEstimatedCubeWeights.collect().map(_.colStats).toSet

    // Initialize global column stats
    var globalColStats: Seq[ColStats] =
      initializeColStats(revision.columnTransformers.map(_.columnName), schema)

    // Update global column stats
    partitionColumnStats.foreach { partitionColStats =>
      globalColStats = globalColStats.zip(partitionColStats).map { case (global, local) =>
        mergedColStats(global, local)
      }
    }
    val globalTransformations = updatedTransformations(globalColStats)

    val dimensionCount = revision.transformations.size
    val spark = SparkSession.active
    import spark.implicits._
    val globalCubeWeights = partitionedEstimatedCubeWeights
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
              cubePartitionCoordinates.foldLeft(1.0)((vol, range) => vol * (range._2 - range._1))

            val cubeGlobalCoordinates = {
              cubePartitionCoordinates
                .zip(colStats.zip(globalColStats))
                .map { case ((l, r), (local, global)) =>
                  toGlobalCoordinates(l, r, local, global)
                }
            }

            var cubeCandidates = Seq(CubeId.root(dimensionCount))
            (0 until cube.depth).foreach { _ =>
              cubeCandidates = cubeCandidates.flatMap(c => c.children)
            }

            cubeCandidates.foreach { candidate =>
              val candidateCoordinates = candidate.from.coordinates.zip(candidate.to.coordinates)
              val dimensionOverlaps = candidateCoordinates.zip(cubeGlobalCoordinates).map {
                case ((candidateFrom, candidateTo), (cubeFrom, cubeTo)) =>
                  if (candidateFrom < cubeTo && cubeFrom < candidateTo) {
                    (candidateTo - cubeFrom).min(cubeTo - candidateFrom)
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
    (globalCubeWeights, globalTransformations)
  }

  override def analyze(
      dataFrame: DataFrame,
      indexStatus: IndexStatus,
      isReplication: Boolean): (DataFrame, TableChanges, IISeq[Transformation]) = {
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

    // Map partition cubes and weights to global cubes and weights
    val (adjustedCubeWeights, transformations) =
      toGlobalCubeWeights(
        partitionedEstimatedCubeWeights,
        indexStatus.revision,
        weightedDataFrame.schema)
    val lastRevision = indexStatus.revision.copy(transformations = transformations)

    // Third, compute the overall estimated cube weights
    val estimatedCubeWeights =
      adjustedCubeWeights
        .transform(estimateCubeWeights(lastRevision))
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

    (weightedDataFrame, tableChanges, transformations)
  }

  def toGlobalCoordinates(
      from: Double,
      to: Double,
      partitionStats: ColStats,
      globalStats: ColStats): (Double, Double) = {
    assert(
      partitionStats.colName == globalStats.colName && partitionStats.dType == globalStats.dType)

    globalStats match {
      case ColStats(_, "StringDataType", _, _) => (from, to)
      case ColStats(_, _, min, max) =>
        val partitionScale = partitionStats.max - partitionStats.min
        val globalScale = max - min
        val fromValue = from * partitionScale + partitionStats.min
        val toValue = to * partitionScale + partitionStats.min
        ((fromValue - min) / globalScale, (toValue - min) / globalScale)

    }

  }

//  def getPartitionColStats(
//      iter: Iterator[Row],
//      columnsToIndex: Seq[String],
//      schema: StructType): Seq[ColStats] = {
//    var partitionColStats = initializeColStats(columnsToIndex, schema)
//    iter.foreach { row =>
//      partitionColStats = partitionColStats.map { stats =>
//        if (stats.dType == "StringDataType") {
//          stats
//        } else {
//          val value = stats match {
//            case ColStats(colName, "DoubleDataType", _, _) =>
//              row.getAs[Double](colName)
//            case ColStats(colName, "IntegerDataType", _, _) =>
//              row.getAs[Int](colName).toDouble
//            case ColStats(colName, "FloatDataType", _, _) =>
//              row.getAs[Float](colName).toDouble
//            case ColStats(colName, "DecimalDataType", _, _) =>
//              row.getAs[Double](colName)
//            case ColStats(colName, "LongDataType", _, _) =>
//              row.getAs[Long](colName).toDouble
//            case _ => throw new RuntimeException(s"Type currently not supported")
//          }
//          stats.copy(min = stats.min.min(value), max = stats.max.max(value))
//        }
//      }
//    }
//    partitionColStats
//  }

  def initializeColStats(columnsToIndex: Seq[String], schema: StructType): Seq[ColStats] = {
    columnsToIndex.map { name =>
      ColStats(
        name,
        SparkRevisionFactory.getColumnQType(name, schema).name,
        Double.MaxValue,
        Double.MinValue)
    }
  }

  def mergedColStats(global: ColStats, local: ColStats): ColStats = {
    assert(global.colName == local.colName && global.dType == local.dType)
    global match {
      case ColStats(_, "StringDataType", _, _) => global
      case ColStats(_, _, min, max) =>
        global.copy(min = min.min(local.min), max = max.max(local.max))
    }
  }

//  def updatedColStats(stats: ColStats, row: Row): ColStats = stats match {
//    case ColStats(_, "StringDataType", _, _) => stats
//    case ColStats(colName, _, min, max) =>
//      stats.copy(min = min.min(row.getAs[Long](colName)), max = max.max(row.getAs[Long](colName)))
//  }

  def updatedTransformations(columnStats: Seq[ColStats]): IISeq[Transformation] = {
    columnStats.map {
      case ColStats(_, "StringDataType", _, _) => HashTransformation()
      case ColStats(_, dType, min: Double, max: Double) =>
        LinearTransformation(min, max, OrderedDataType(dType))
    }.toIndexedSeq
  }

}

case class ColStats(colName: String, dType: String, min: Double, max: Double)

case class CubeWeightAndStats(
    cubeBytes: Array[Byte],
    normalizedWeight: NormalizedWeight,
    colStats: Seq[ColStats])
