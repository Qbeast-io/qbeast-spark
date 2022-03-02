/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.IISeq
import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.transform.{HashTransformation, LinearTransformation, Transformation}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

object SinglePassColStatsUtils {

  val supportedTypes = Set(
    "DoubleDataType",
    "DecimalDataType",
    "StringDataType",
    "IntegerDataType",
    "FloatDataType",
    "LongDataType")

  def initializeColStats(columnsToIndex: Seq[String], schema: StructType): Seq[ColStats] = {
    columnsToIndex.map { name =>
      val dType = SparkRevisionFactory.getColumnQType(name, schema).name
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
      }
      stats.copy(min = stats.min.min(value), max = stats.max.max(value))
    }
  }

  def getTransformations(columnStats: Seq[ColStats]): IISeq[Transformation] = {
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
