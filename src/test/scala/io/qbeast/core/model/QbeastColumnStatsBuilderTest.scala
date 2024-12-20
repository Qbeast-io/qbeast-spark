package io.qbeast.core.model

import io.qbeast.core.transform.CDFQuantilesTransformer
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

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

}
