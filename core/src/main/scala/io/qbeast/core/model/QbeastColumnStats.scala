package io.qbeast.core.model

import io.qbeast.core.transform.CDFNumericQuantilesTransformer
import io.qbeast.core.transform.CDFStringQuantilesTransformer
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.core.transform.Transformer
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * Container for Qbeast Column Stats
 *
 * @param columnStatsSchema
 *   the column stats schema
 * @param columnStatsRow
 *   the column stats row
 */
case class QbeastColumnStats(columnStatsSchema: StructType, columnStatsRow: Row)

object QbeastColumnStatsBuilder {

  /**
   * Builds the column stats schema
   *
   * For each column transformer, it creates a StructField with it's stats names
   * @param dataSchema
   *   the data schema
   * @param columnTransformers
   *   the column transformers
   * @return
   */
  def buildColumnStatsSchema(
      dataSchema: StructType,
      columnTransformers: Seq[Transformer]): StructType = {
    val columnStatsSchema = StructType(columnTransformers.flatMap { transformer =>
      val transformerStatsNames = transformer.stats.statsNames
      val transformerColumnName = transformer.columnName
      val sparkDataType = dataSchema.find(_.name == transformerColumnName) match {
        case Some(field) => field.dataType
        case None =>
          throw AnalysisExceptionFactory.create(
            s"Column $transformerColumnName not found in the data schema")
      }
      transformer match {
        case LinearTransformer(_, _) =>
          transformerStatsNames.map(statName =>
            StructField(statName, sparkDataType, nullable = true))
        case CDFNumericQuantilesTransformer(_, _) =>
          transformerStatsNames.map(statName =>
            StructField(statName, ArrayType(DoubleType), nullable = true))
        case CDFStringQuantilesTransformer(_) =>
          transformerStatsNames.map(statName =>
            StructField(statName, ArrayType(StringType), nullable = true))
        case _ => // TODO: Add support for other transformers
          transformerStatsNames.map(statName =>
            StructField(statName, StringType, nullable = true))
      }
    })
    columnStatsSchema
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

  def buildColumnStatsRow(stats: String, columnStatsSchema: StructType): Row = {
    // If the stats are empty, return an empty row
    if (stats.isEmpty) return Row.empty
    // Otherwise, parse the stats
    val spark = SparkSession.active
    import spark.implicits._
    val columnStatsJSON = Seq(stats).toDS()
    val row = spark.read
      .option("inferTimestamp", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
      .schema(columnStatsSchema)
      .json(columnStatsJSON)
      .first()
    // If the stats are non-empty, and the row values are null,
    // we assume that the stats are not in the correct format
    val areAllStatsNull = row.toSeq.forall(f => f == null)
    if (stats.nonEmpty && areAllStatsNull) {
      throw AnalysisExceptionFactory.create(
        s"The columnStats provided is not a valid JSON: $stats")
    }
    // return row
    row
  }

  /**
   * Builds the QbeastColumnStats
   *
   * @param statsString
   *   the stats in a JSON string
   * @param columnTransformers
   *   the set of columnTransformers to build the Stats from
   * @param dataSchema
   *   the data schema to build the Stats from
   * @return
   */
  def build(
      statsString: String,
      columnTransformers: Seq[Transformer],
      dataSchema: StructType): QbeastColumnStats = {
    val columnStatsSchema = buildColumnStatsSchema(dataSchema, columnTransformers)
    val columnStatsRow = buildColumnStatsRow(statsString, columnStatsSchema)
    QbeastColumnStats(columnStatsSchema, columnStatsRow)
  }

}
