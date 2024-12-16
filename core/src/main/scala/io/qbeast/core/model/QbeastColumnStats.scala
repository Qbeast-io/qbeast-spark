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

case class QbeastColumnStats(stats: String, columnStatsSchema: StructType, columnStatsRow: Row) {}

object QbeastColumnStats {

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
      transformer match {
        case LinearTransformer(columnName, _) =>
          val sparkDataType = dataSchema.find(_.name == columnName).get.dataType
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
    println("columnStats schema")
    println(columnStatsSchema)
    columnStatsSchema
  }

  def buildColumnStatsRow(stats: String, columnStatsSchema: StructType): Row = {
    try {
      val spark = SparkSession.active
      import spark.implicits._
      val columnStatsJSON = Seq(stats).toDS()
      val row = spark.read
        .option("inferTimestamp", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
        .schema(columnStatsSchema)
        .json(columnStatsJSON)
        .first()

      println("columnStats row")
      println(row)
      row
    } catch {
      case _: Throwable =>
        throw AnalysisExceptionFactory.create(
          "The column stats provided are not in the correct format")
    }
  }

  def build(
      stats: String,
      columnTransformers: Seq[Transformer],
      dataSchema: StructType): QbeastColumnStats = {
    val columnStatsSchema = buildColumnStatsSchema(dataSchema, columnTransformers)
    val columnStatsRow = buildColumnStatsRow(stats, columnStatsSchema)
    QbeastColumnStats(stats, columnStatsSchema, columnStatsRow)
  }

}
