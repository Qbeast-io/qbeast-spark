package io.qbeast.core.model

import io.qbeast.core.transform._
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.types._
import org.apache.spark.sql.AnalysisException

import scala.annotation.nowarn

@nowarn("cat=deprecation")
class QbeastColumnStatsTest extends QbeastIntegrationTestSpec {

  "QbeastColumnStats" should "build the stats schema and row correctly" in withSpark { _ =>
    val columnTransformers = Seq(
      LinearTransformer("int_col", IntegerDataType),
      LinearTransformer("float_col", FloatDataType),
      LinearTransformer("long_col", LongDataType),
      LinearTransformer("double_col", DoubleDataType),
      CDFNumericQuantilesTransformer("int_col_2", IntegerDataType),
      CDFNumericQuantilesTransformer("float_col_2", FloatDataType),
      CDFNumericQuantilesTransformer("long_col_2", LongDataType),
      CDFNumericQuantilesTransformer("double_col_2", DoubleDataType),
      CDFStringQuantilesTransformer("string_col_2"),
      StringHistogramTransformer("string_col_3", StringDataType))

    val statsSchema = StructType(
      StructField("int_col_min", IntegerType) ::
        StructField("int_col_max", IntegerType) ::
        StructField("float_col_min", FloatType) ::
        StructField("float_col_max", FloatType) ::
        StructField("long_col_min", LongType) ::
        StructField("long_col_max", LongType) ::
        StructField("double_col_min", DoubleType) ::
        StructField("double_col_max", DoubleType) ::
        StructField("int_col_2_quantiles", ArrayType(DoubleType)) ::
        StructField("float_col_2_quantiles", ArrayType(DoubleType)) ::
        StructField("long_col_2_quantiles", ArrayType(DoubleType)) ::
        StructField("double_col_2_quantiles", ArrayType(DoubleType)) ::
        StructField("string_col_2_quantiles", ArrayType(StringType)) ::
        StructField("string_col_3_histogram", ArrayType(StringType)) :: Nil)

    val statsString = """{"int_col_min":0,"int_col_max":1,
        |"float_col_min":0.0,"float_col_max":1.0,
        |"long_col_min":0,"long_col_max":1,
        |"double_col_min":0.0,"double_col_max":1.0,
        |"int_col_2_quantiles":[1.0, 2.0],
        |"float_col_2_quantiles":[0.1, 0.2],
        |"long_col_2_quantiles":[1.0, 2.0],
        |"double_col_2_quantiles":[0.1, 0.2],
        |"string_col_2_quantiles":["a", "b"],
        |"string_col_3_histogram":["a", "b"]}""".stripMargin

    val columnStats = QbeastColumnStats(statsString, columnTransformers)
    columnStats.schema shouldBe statsSchema

    val statsRow = columnStats.rowOption.get
    assert(statsSchema.fields.length != 0)
    statsRow.getAs[Int]("int_col_min") shouldBe 0
    statsRow.getAs[Int]("int_col_max") shouldBe 1
    statsRow.getAs[Float]("float_col_min") shouldBe 0.0
    statsRow.getAs[Float]("float_col_max") shouldBe 1.0
    statsRow.getAs[Long]("long_col_min") shouldBe 0
    statsRow.getAs[Long]("long_col_max") shouldBe 1
    statsRow.getAs[Double]("double_col_min") shouldBe 0.0
    statsRow.getAs[Double]("double_col_max") shouldBe 1.0
    statsRow.getAs[Array[Double]]("int_col_2_quantiles") shouldBe Array(1.0, 2.0)
    statsRow.getAs[Array[Double]]("float_col_2_quantiles") shouldBe Array(0.1, 0.2)
    statsRow.getAs[Array[Double]]("long_col_2_quantiles") shouldBe Array(1.0, 2.0)
    statsRow.getAs[Array[Double]]("double_col_2_quantiles") shouldBe Array(0.1, 0.2)
    statsRow.getAs[Array[String]]("string_col_2_quantiles") shouldBe Array("a", "b")
    statsRow.getAs[Array[String]]("string_col_3_histogram") shouldBe Array("a", "b")
  }

  it should "build the stats row correctly if no stats are specified" in withSpark { _ =>
    val columnTransformers = Seq(LinearTransformer("int_col", IntegerDataType))
    val statsSchema = StructType(
      StructField("int_col_min", IntegerType) ::
        StructField("int_col_max", IntegerType) :: Nil)
    val columnStats = QbeastColumnStats("", columnTransformers)
    columnStats.schema shouldBe statsSchema
    columnStats.rowOption shouldBe None
  }

  it should "return None when no stats is provided" in withSpark { _ =>
    val columnTransformers = Seq(
      LinearTransformer("int_col", IntegerDataType),
      CDFNumericQuantilesTransformer("int_col_2", IntegerDataType),
      CDFStringQuantilesTransformer("string_col_2"),
      StringHistogramTransformer("string_col_3", StringDataType))
    val columnStats = QbeastColumnStats("", columnTransformers)
    columnTransformers.flatMap(columnStats.createTransformation).isEmpty shouldBe true
  }

  it should "build the stats schema and ignore redundant stats names" in withSpark { _ =>
    val columnTransformers = Seq(LinearTransformer("int_col", IntegerDataType))
    val statsSchema = StructType(
      StructField("int_col_min", IntegerType) ::
        StructField("int_col_max", IntegerType) :: Nil)
    val statsString = """{"int_col_min": 100, "int_col_max": 200, "some_redundant_name": 100}"""
    val columnStats = QbeastColumnStats(statsString, columnTransformers)
    columnStats.schema shouldBe statsSchema
    columnStats.rowOption.get.schema.find(_.name == "some_redundant_name") shouldBe None
  }

  it should "throw an exception when the provided JSON is invalid" in withSpark { _ =>
    an[AnalysisException] shouldBe thrownBy {
      QbeastColumnStats(
        """{"int_col_min":0,"int_col_max":1""",
        Seq(LinearTransformer("int_col", IntegerDataType)))
    }
    an[AnalysisException] shouldBe thrownBy {
      QbeastColumnStats(
        """{"long_col_quantiles":[0.0, 25.0, 50.0, 75.0, 100.0}""",
        Seq(LinearTransformer("long_col", IntegerDataType)))
    }
    an[AnalysisException] shouldBe thrownBy {
      QbeastColumnStats(
        """{"long_col_quantiles":[0.0, 25.0, 50.0, 75.0, 100]}""",
        Seq(LinearTransformer("long_col", IntegerDataType)))
    }
    an[AnalysisException] shouldBe thrownBy {
      QbeastColumnStats(
        """{"string_col_quantiles":["a", "b", "c", "d"}""",
        Seq(LinearTransformer("string_col", StringDataType)))
    }
    // JSON does not support float parsing with f termination
    an[AnalysisException] shouldBe thrownBy {
      QbeastColumnStats(
        """{"float_col_min":0.0f,"float_col_max":0.0f}""",
        Seq(LinearTransformer("float_col", FloatDataType)))
    }

  }

  it should "build the QbeastColumnStats for string histogram (deprecated)" in withSpark { _ =>
    val columnTransformers = Seq(StringHistogramTransformer("string_col", StringDataType))
    val statsString = """{"string_col_histogram":["a", "b", "c", "d", "e"]}"""
    val qbeastColumnStats = QbeastColumnStats(statsString, columnTransformers)

    qbeastColumnStats.schema shouldBe StructType(
      StructField("string_col_histogram", ArrayType(StringType)) :: Nil)
    qbeastColumnStats.rowOption.get.getAs[Array[String]]("string_col_histogram") shouldBe
      Array("a", "b", "c", "d", "e")
  }

  "createTransformation" should "create the right Transformations" in withSpark { _ =>
    val columnTransformers = Seq(
      LinearTransformer("int_col", IntegerDataType),
      CDFNumericQuantilesTransformer("int_col_2", IntegerDataType),
      CDFStringQuantilesTransformer("string_col"),
      StringHistogramTransformer("string_col_2", StringDataType))
    val statsString =
      """{"int_col_min":0,"int_col_max":10,
        |"int_col_2_quantiles":[0.0,25.0,50.0,75.0,100.0],
        |"string_col_quantiles": ["a","b","c", "d"],
        |"string_col_2_histogram": ["a","b","c","d"]}""".stripMargin
    val columnStats = QbeastColumnStats(statsString, columnTransformers)
    columnStats.createTransformation(columnTransformers.head) should matchPattern {
      case Some(LinearTransformation(0, 10, _, IntegerDataType)) =>
    }
    columnStats
      .createTransformation(columnTransformers(1)) shouldBe Some(
      CDFNumericQuantilesTransformation(Vector(0.0, 25.0, 50.0, 75.0, 100.0), IntegerDataType))
    columnStats
      .createTransformation(columnTransformers(2)) shouldBe Some(
      CDFStringQuantilesTransformation(Vector("a", "b", "c", "d")))
    columnStats
      .createTransformation(columnTransformers.last) shouldBe Some(
      StringHistogramTransformation(Vector("a", "b", "c", "d")))
  }

  it should "throw an exception when the provided stats are not valid for a LinearTransformer" in
    withSpark { _ =>
      val t = LinearTransformer("int_col", IntegerDataType)
      an[IllegalArgumentException] shouldBe thrownBy {
        val columnStats = QbeastColumnStats("""{"int_col_min":0}""", Seq(t))
        columnStats.createTransformation(t)
      }
      an[IllegalArgumentException] shouldBe thrownBy {
        val columnStats = QbeastColumnStats("""{"int_col_max":1}""", Seq(t))
        columnStats.createTransformation(t)
      }
      an[IllegalArgumentException] shouldBe thrownBy {
        val columnStats = QbeastColumnStats("""{"int_col_min":0,"int_col_max":-1}""", Seq(t))
        columnStats.createTransformation(t)
      }
      an[IllegalArgumentException] shouldBe thrownBy {
        val columnStats = QbeastColumnStats("""{"int_col_min":0,"int_col_max":0}""", Seq(t))
        columnStats.createTransformation(t)
      }
    }

  it should "throw an exception when the provided quantiles are incorrect" in withSpark { _ =>
    an[IllegalArgumentException] shouldBe thrownBy {
      // empty numeric quantiles
      val t = CDFNumericQuantilesTransformer("int_col", IntegerDataType)
      val columnStats = QbeastColumnStats("""{"int_col_quantiles":[]}""", Seq(t))
      columnStats.createTransformation(t)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      // numeric quantiles with a single value
      val t = CDFNumericQuantilesTransformer("int_col", IntegerDataType)
      val columnStats = QbeastColumnStats("""{"int_col_quantiles":[0.0]}""", Seq(t))
      columnStats.createTransformation(t)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      // empty string quantiles
      val t = CDFStringQuantilesTransformer("string_col")
      val columnStats = QbeastColumnStats("""{"string_col_quantiles":[]}""", Seq(t))
      columnStats.createTransformation(t)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      // string quantiles with a single value
      val t = CDFStringQuantilesTransformer("string_col")
      val columnStats = QbeastColumnStats("""{"string_col_quantiles":["a"]}""", Seq(t))
      columnStats.createTransformation(t)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      // empty string histogram
      val t = StringHistogramTransformer("string_col", StringDataType)
      val columnStats = QbeastColumnStats("""{"string_col_histogram":[]}""", Seq(t))
      columnStats.createTransformation(t)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      // string histogram with a single value
      val t = StringHistogramTransformer("string_col", StringDataType)
      val columnStats = QbeastColumnStats("""{"string_col_histogram":["a"]}""", Seq(t))
      columnStats.createTransformation(t)
    }
  }

}
