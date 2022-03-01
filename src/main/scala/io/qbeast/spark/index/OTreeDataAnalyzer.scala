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
      // scalastyle:off println
      println(">>> estimatePartitionCubeWeights")
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

      println(">>> estimatePartitionCubeWeights -> Entering mapPartitions!!!")

      selected
        .mapPartitions(rows => {
          val (iterForStats, iterForCubeWeights) = rows.duplicate

          println(">>> estimatePartitionCubeWeights -> Iterating through iterForStats")
          iterForStats.foreach { row =>
            partitionColStats = partitionColStats.map(stats => updatedColStats(stats, row))
          }
          println(s"ColStats after update: $partitionColStats")

          val weights =
            new CubeWeightsBuilder(
              indexStatus = indexStatus,
              numPartitions = numPartitions,
              numElements = numElements,
              bufferCapacity = bufferCapacity)

          val partitionRevision =
            revision.copy(transformations = updatedTransformations(partitionColStats))

          println(">>> estimatePartitionCubeWeights -> Iterating through iterForCubeWeights")

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
    // scalastyle:off println
    println(">>> toGlobalCubeWeights")
    val partitionColumnStats: Set[Seq[ColStats]] =
      partitionedEstimatedCubeWeights.collect().map(_.colStats).toSet

    val colsToIndex = revision.columnTransformers.map(_.columnName)
    // Initialize global column stats
    var globalColStats: Seq[ColStats] =
      initializeColStats(colsToIndex, schema)

    // Update global column stats
    partitionColumnStats.foreach { partitionColStats =>
      globalColStats = globalColStats.zip(partitionColStats).map { case (global, local) =>
        mergedColStats(global, local)
      }
    }

    val globalTransformations = updatedTransformations(globalColStats)

    val dimensionCount = colsToIndex.size

    val spark = SparkSession.active
    import spark.implicits._

    val globalCubeWeights = partitionedEstimatedCubeWeights
      .mapPartitions { iter =>
        val partitionCubeWeights = mutable.ArrayBuffer[CubeNormalizedWeight]()
        iter.foreach {
          case CubeWeightAndStats(
                cubeBytes: Array[Byte],
                normalizedWeight: NormalizedWeight,
                colStats: Seq[ColStats]) =>
            val cube = CubeId(dimensionCount, cubeBytes)

            val cubeGlobalCoordinates = 0
              .until(dimensionCount)
              .map(i =>
                toGlobalCoordinates(
                  cube.from.coordinates(i),
                  cube.to.coordinates(i),
                  colStats(i),
                  globalColStats(i)))

            var cubeCandidates = Seq(CubeId.root(dimensionCount))
            (0 until cube.depth).foreach { _ =>
              cubeCandidates = cubeCandidates.flatMap(c => c.children)
            }

            cubeCandidates foreach (c => assert(c.depth == cube.depth))

            val cubeOverlaps = mutable.ArrayBuffer[Double]()
            cubeCandidates.foreach { candidate =>
              var isOverlapping = true
              val candidateCoordinates = candidate.from.coordinates.zip(candidate.to.coordinates)
              val dimensionOverlaps = candidateCoordinates.zip(cubeGlobalCoordinates).map {
                case ((candidateFrom, candidateTo), (cubeFrom, cubeTo)) =>
                  if (candidateFrom < cubeTo && cubeFrom < candidateTo) {
                    val cubeDimWidth = cubeTo - cubeFrom
                    val ol =
                      (candidateTo - cubeFrom)
                        .min(cubeTo - candidateFrom)
                        .min(cubeDimWidth) / cubeDimWidth
                    if (candidateFrom != cubeFrom || candidateTo != cubeTo) {
                      println(s""">>> Overlap:
                           |candidate range: ($candidateFrom, $candidateTo)
                           |cube range: ($cubeFrom, $cubeTo)
                           |overlap: ${ol * cubeDimWidth}, cube dim size: $cubeDimWidth
                           |""".stripMargin)
                    }
                    ol
                  } else {
                    isOverlapping = false
                    0.0
                  }
              }
              if (isOverlapping) {
                val candidateOverlap = dimensionOverlaps
                  .foldLeft(1.0)((acc, dimOverlap) => acc * dimOverlap)
                cubeOverlaps += candidateOverlap
                val overlappingWeight = normalizedWeight * candidateOverlap
                println(s"""
                     |cubeWeight: $normalizedWeight,
                     |candidateWeight: $overlappingWeight,
                     |overlap: $candidateOverlap""".stripMargin)
                println()
                partitionCubeWeights += CubeNormalizedWeight(cubeBytes, overlappingWeight)
              }
            }
            assert(cubeOverlaps.sum == 1.0)
        }
        println(s"Number of overlapping candidates for partition: ${partitionCubeWeights.size}")
        println()
        println()
        println()
        partitionCubeWeights.toIterator
      }
    (globalCubeWeights, globalTransformations)
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

    // Map partition cubes and weights to global cubes and weights
    val (globalEstimatedCubeWeights, transformations) =
      toGlobalCubeWeights(
        partitionedEstimatedCubeWeights,
        indexStatus.revision,
        weightedDataFrame.schema)
    val lastRevision = indexStatus.revision.copy(transformations = transformations)

    // Third, compute the overall estimated cube weights
    val estimatedCubeWeights =
      globalEstimatedCubeWeights
        .transform(estimateCubeWeights(lastRevision))
        .collect()
        .toMap

    println()
    println()
    println()
    println()
    println()
    val cubeWeights = estimatedCubeWeights.values.toList.sorted
    val l = cubeWeights.size
    Seq(
      cubeWeights.head,
      cubeWeights((l * 0.25).toInt),
      cubeWeights((l * 0.5).toInt),
      cubeWeights((l * 0.75).toInt),
      cubeWeights.last) foreach println

    // Gather the new changes
    val tableChanges = TableChanges(
      None,
      IndexStatusChange(
        indexStatus.copy(revision = lastRevision),
        estimatedCubeWeights,
        deltaReplicatedSet =
          if (isReplication) indexStatus.cubesToOptimize
          else Set.empty[CubeId]))

    (weightedDataFrame, tableChanges)
  }

  def toGlobalCoordinates(
      from: Double,
      to: Double,
      local: ColStats,
      global: ColStats): (Double, Double) = {
    assert(local.colName == global.colName && local.dType == global.dType)
    if (global.dType == "StringDataType" || global.min == local.min && global.max == local.max) {
      (from, to)
    } else {
      val (gScale, lScale) = (global.max - global.min, local.max - local.min)
      val scale = lScale / gScale
      val offset = (local.min - global.min) / gScale
      val (newFrom, newTo) = (from * scale + offset, to * scale + offset)
      if (from != newFrom || to != newTo) {
        println(s"(from,to): ($from, $to), (newFrom,newTo): ($newFrom, $newTo)")
      }
      (newFrom, newTo)
    }
  }

  def initializeColStats(columnsToIndex: Seq[String], schema: StructType): Seq[ColStats] = {
    columnsToIndex.map { name =>
      val dType = SparkRevisionFactory.getColumnQType(name, schema).name
      val supportedTypes = Set(
        "DoubleDataType",
        "DecimalDataType",
        "StringDataType",
        "IntegerDataType",
        "FloatDataType",
        "LongDataType")
      require(supportedTypes.contains(dType), s"Type: $dType is not currently supported.")
      ColStats(name, dType)
    }
  }

  def mergedColStats(global: ColStats, local: ColStats): ColStats = {
    assert(global.colName == local.colName && global.dType == local.dType)
    if (global.dType == "StringDataType") {
      global
    } else {
      global.copy(min = global.min.min(local.min), max = global.max.max(local.max))
    }
  }

  def updatedColStats(stats: ColStats, row: Row): ColStats = {
    if (stats.dType == "StringDataType") {
      stats
    } else {
      val value = stats.dType match {
        case "DoubleDataType" | "DecimalDataType" => row.getAs[Double](stats.colName)
        case "IntegerDataType" => row.getAs[Int](stats.colName).asInstanceOf[Double]
        case "FloatDataType" => row.getAs[Float](stats.colName).asInstanceOf[Double]
        case "LongDataType" => row.getAs[Long](stats.colName).asInstanceOf[Double]
//        case _ => throw new RuntimeException(s"Type currently not supported")
      }
      stats.copy(min = stats.min.min(value), max = stats.max.max(value))
    }
  }

  def updatedTransformations(columnStats: Seq[ColStats]): IISeq[Transformation] = {
    columnStats.map { stats =>
      if (stats.dType == "StringDataType") {
        HashTransformation()
      } else {
        val (minNumber, maxNumber) = stats.dType match {
          case "DoubleDataType" | "DecimalDataType" =>
            (stats.min, stats.max)
          case "IntegerDataType" =>
            (stats.min.asInstanceOf[Int], stats.max.asInstanceOf[Int])
          case "FloatDataType" =>
            (stats.min.asInstanceOf[Float], stats.max.asInstanceOf[Float])
          case "LongDataType" =>
            (stats.min.asInstanceOf[Long], stats.max.asInstanceOf[Long])
        }
        LinearTransformation(minNumber, maxNumber, OrderedDataType(stats.dType))
      }
    }.toIndexedSeq
  }

}

case class ColStats(
    colName: String,
    dType: String,
    min: Double = Double.MaxValue,
    max: Double = Double.MinValue)

case class CubeWeightAndStats(
    cubeBytes: Array[Byte],
    normalizedWeight: NormalizedWeight,
    colStats: Seq[ColStats])
