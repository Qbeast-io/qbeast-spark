package io.qbeast.core.model

import io.qbeast.core.transform.CDFQuantilesTransformer
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.core.transform.StringHistogramTransformer
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row

import scala.annotation.nowarn

@nowarn("cat=deprecation")
class QbeastColumnStatsBuilderTest extends QbeastIntegrationTestSpec {

  "QbeastColumnStats" should "build the schema for linear transformations" in withSpark { _ =>
    val dataSchema =
      StructType(
        Seq(
          StructField("int_col", IntegerType),
          StructField("float_col", FloatType),
          StructField("long_col", LongType),
          StructField("double_col", DoubleType)))
    val columnTransformers = Seq(
      LinearTransformer("int_col", IntegerDataType),
      LinearTransformer("float_col", FloatDataType),
      LinearTransformer("long_col", LongDataType),
      LinearTransformer("double_col", DoubleDataType))

    val statsString =
      """{"int_col_min":0,"int_col_max":0,
        |"float_col_min":0.0,"float_col_max":0.0,
        |"long_col_min":0,"long_col_max":0,
        |"double_col_min":0.0,"double_col_max":0.0}""".stripMargin
    val qbeastColumnStats =
      QbeastColumnStatsBuilder.build(statsString, columnTransformers, dataSchema)
    val qbeastColumnStatsSchema = qbeastColumnStats.columnStatsSchema
    val qbeastColumnStatsRow = qbeastColumnStats.columnStatsRow

    assert(qbeastColumnStatsSchema.fields.length != 0)
    qbeastColumnStatsRow.getAs[Int]("int_col_min") shouldBe 0
    qbeastColumnStatsRow.getAs[Int]("int_col_max") shouldBe 0
    qbeastColumnStatsRow.getAs[Float]("float_col_min") shouldBe 0.0
    qbeastColumnStatsRow.getAs[Float]("float_col_max") shouldBe 0.0
    qbeastColumnStatsRow.getAs[Long]("long_col_min") shouldBe 0
    qbeastColumnStatsRow.getAs[Long]("long_col_max") shouldBe 0
    qbeastColumnStatsRow.getAs[Double]("double_col_min") shouldBe 0.0
  }

  it should "build the schema for quantiles" in withSpark { spark =>
    val dataSchema =
      StructType(Seq(StructField("int_col", IntegerType), StructField("string_col", StringType)))
    val columnTransformers = Seq(CDFQuantilesTransformer("int_col", IntegerDataType))
    val statsString = """{"int_col_quantiles":[0.0,0.25,0.5,0.75,1.0]}"""
    val qbeastColumnStats =
      QbeastColumnStatsBuilder.build(statsString, columnTransformers, dataSchema)

    qbeastColumnStats.columnStatsSchema shouldBe StructType(
      StructField("int_col_quantiles", ArrayType(DoubleType), nullable = true) :: Nil)
    qbeastColumnStats.columnStatsRow.getAs[Array[Double]]("int_col_quantiles") shouldBe
      Array(0.0, 0.25, 0.5, 0.75, 1.0)

  }

  it should "build the schema for string quantiles" in withSpark(spark => {
    val dataSchema =
      StructType(Seq(StructField("string_col", StringType)))
    val columnTransformers = Seq(CDFQuantilesTransformer("string_col", StringDataType))
    val statsString = """{"string_col_quantiles":["a","b","c","d","e"]}"""
    val qbeastColumnStats =
      QbeastColumnStatsBuilder.build(statsString, columnTransformers, dataSchema)

    qbeastColumnStats.columnStatsSchema shouldBe StructType(
      StructField("string_col_quantiles", ArrayType(StringType), nullable = true) :: Nil)
    qbeastColumnStats.columnStatsRow.getAs[Array[String]]("string_col_quantiles") shouldBe
      Array("a", "b", "c", "d", "e")
  })

  it should "build the QbeastColumnStats for string histogram (deprecated)" in withSpark(
    spark => {
      val dataSchema =
        StructType(Seq(StructField("string_col", StringType)))
      val columnTransformers = Seq(StringHistogramTransformer("string_col", StringDataType))
      val statsString = """{"string_col_histogram":["a", "b", "c", "d", "e"]}"""
      val qbeastColumnStats =
        QbeastColumnStatsBuilder.build(statsString, columnTransformers, dataSchema)

      qbeastColumnStats.columnStatsSchema shouldBe StructType(
        StructField("string_col_histogram", ArrayType(StringType), nullable = true) :: Nil)
      qbeastColumnStats.columnStatsRow.getAs[Array[String]]("string_col_histogram") shouldBe
        Array("a", "b", "c", "d", "e")
    })

  it should "throw error when parsing is incorrect" in withSpark(spark => {
    val dataSchema =
      StructType(Seq(StructField("int_col", IntegerType), StructField("float_col", FloatType)))
    val columnTransformers = Seq(CDFQuantilesTransformer("int_col", IntegerDataType))
    val statsString = """{"int_col_quantiles":0.0,0.25,0.5,0.75,1.0]}"""
    an[AnalysisException] shouldBe thrownBy {
      QbeastColumnStatsBuilder.build(statsString, columnTransformers, dataSchema)
    }

    val columnTransformersLinear = Seq(LinearTransformer("float_col", FloatDataType))
    val statsStringLinear =
      """{"float_col_min":0.0f,"float_col_max":0.0f}""" // JSON does not support float parsing with f termination
    an[AnalysisException] shouldBe thrownBy {
      QbeastColumnStatsBuilder.build(statsStringLinear, columnTransformersLinear, dataSchema)
    }

  })

  it should "only process LinearTransformers and QuantilesTransformers" in withSpark(spark => {
    val dataSchema =
      StructType(
        Seq(
          StructField("int_col", IntegerType),
          StructField("long_col", LongType),
          StructField("string_col", StringType)))
    val columnTransformers = Seq(
      CDFQuantilesTransformer("int_col", IntegerDataType),
      LinearTransformer("long_col", LongDataType))
    val statsString =
      """{"int_col_quantiles":[0.0,0.25,0.5,0.75,1.0], "long_col_min":0,"long_col_max":0}"""
    val qbeastColumnStats =
      QbeastColumnStatsBuilder.build(statsString, columnTransformers, dataSchema)

    qbeastColumnStats.columnStatsSchema shouldBe StructType(
      Seq(
        StructField("int_col_quantiles", ArrayType(DoubleType), nullable = true),
        StructField("long_col_max", LongType, nullable = true),
        StructField("long_col_min", LongType, nullable = true)
      )
    ) // Ignore String col
    qbeastColumnStats.columnStatsRow.getAs[Array[Double]]("int_col_quantiles") shouldBe
      Array(0.0, 0.25, 0.5, 0.75, 1.0)
    qbeastColumnStats.columnStatsRow.getAs[Long]("long_col_min") shouldBe 0
    qbeastColumnStats.columnStatsRow.getAs[Long]("long_col_max") shouldBe 0

  })

  it should "return empty Row if no columnStats are specified" in withSpark(spark => {
    val dataSchema =
      StructType(
        Seq(
          StructField("int_col", IntegerType),
          StructField("long_col", LongType),
          StructField("string_col", StringType)))
    val columnTransformers = Seq(
      LinearTransformer("int_col", IntegerDataType),
      LinearTransformer("long_col", LongDataType),
      LinearTransformer("string_col", StringDataType))
    val statsString = ""
    val qbeastColumnStats =
      QbeastColumnStatsBuilder.build(statsString, columnTransformers, dataSchema)

    qbeastColumnStats.columnStatsRow shouldBe Row.empty
  })

}