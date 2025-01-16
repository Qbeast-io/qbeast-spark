/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.core.model

import io.qbeast.core.transform._
import io.qbeast.spark.utils.SparkToQTypesUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Container for Qbeast Column Stats
 *
 * @param schema
 *   the column stats schema
 * @param rowOption
 *   the column stats row
 */
case class QbeastColumnStats(schema: StructType, rowOption: Option[Row]) extends Logging {

  /**
   * Creates a transformation for the given transformer. If the stats are not available, it
   * returns None. An IllegalArgumentException is thrown if the output is an
   * IdentityTransformation. Otherwise, the provided stats will be used to create and returns the
   * transformation.
   *
   * @param transformer
   *   the transformer
   * @return
   */
  def createTransformation(transformer: Transformer): Option[Transformation] = rowOption match {
    case Some(row) =>
      val hasStats = transformer.stats.statsNames.exists(row.getAs[Object](_) != null)
      if (hasStats) {
        val transformation = transformer.makeTransformation(row.getAs[Object])
        (transformer, transformation) match {
          case (_: LinearTransformer, _: IdentityTransformation) =>
            // If the transformation is IdentityTransformation, it means:
            // 1. Either columnName_min or columnName_max is missing or
            // 2. columnName_min and columnName_max are the same
            throw new IllegalArgumentException(
              s"The provided columnStats for column ${transformer.columnName} are not valid. " +
                s"Make sure both min and max values are provided and satisfy the condition: min < max.")
          case _ => Some(transformation)
        }
      } else None
    case None => None
  }

}

/**
 * Companion object for QbeastColumnStats
 */
object QbeastColumnStats {

  /**
   * Builds the column stats schema
   *
   * For each column transformer, create the sequence StructField for its column stats
   * @param columnTransformers
   *   the column transformers
   * @return
   */
  private[model] def buildColumnStatsSchema(columnTransformers: Seq[Transformer]): StructType = {
    val builder = Seq.newBuilder[StructField]
    columnTransformers.foreach { t =>
      val fields = t match {
        case lt: LinearTransformer =>
          val sparkDataType = SparkToQTypesUtils.convertToSparkDataType(lt.dataType)
          lt.stats.statsNames.map(StructField(_, sparkDataType, nullable = true))
        case nq: CDFNumericQuantilesTransformer =>
          nq.stats.statsNames.map(StructField(_, ArrayType(DoubleType), nullable = true))
        case sq: CDFStringQuantilesTransformer =>
          sq.stats.statsNames.map(StructField(_, ArrayType(StringType), nullable = true))
        case sh: StringHistogramTransformer =>
          sh.stats.statsNames.map(StructField(_, ArrayType(StringType), nullable = true))
        case _ => Seq.empty
        // TODO: Add support for other transformers
      }
      builder ++= fields
    }
    StructType(builder.result())
  }

  /**
   * Builds the column stats row
   *
   * @param stats
   *   the stats in a JSON string
   * @param columnStatsSchema
   *   the column stats schema
   * @return
   */
  private[model] def buildColumnStatsRow(
      stats: String,
      columnStatsSchema: StructType): Option[Row] = {
    if (stats.isEmpty) None // No stats are provided
    else {
      val spark = SparkSession.active
      import spark.implicits._
      val columnStatsJSON = Seq(stats).toDS()
      val row = spark.read
        .option("inferTimestamp", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
        .schema(columnStatsSchema)
        .json(columnStatsJSON)
        .first()
      // All values will be Null is the input JSON is invalid
      val isInvalidJSON = row.toSeq.forall(_ == null)
      if (isInvalidJSON) {
        throw AnalysisExceptionFactory.create(
          s"The columnStats provided is not a valid JSON: $stats")
      }
      Some(row)
    }
  }

  /**
   * Builds the QbeastColumnStats
   *
   * @param statsString
   *   the stats in a JSON string
   * @param columnTransformers
   *   the set of columnTransformers to build the Stats from
   * @return
   */
  def apply(statsString: String, columnTransformers: Seq[Transformer]): QbeastColumnStats = {
    val statsSchema = buildColumnStatsSchema(columnTransformers)
    val statsRowOption = buildColumnStatsRow(statsString, statsSchema)
    QbeastColumnStats(statsSchema, statsRowOption)
  }

}
