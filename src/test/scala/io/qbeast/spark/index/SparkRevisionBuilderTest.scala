package io.qbeast.spark.index

import io.qbeast.model._
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.transform.{HashTransformer, LinearTransformer}
case class Test(a: Int, b: Double, c: String, d: Float)

class SparkRevisionBuilderTest extends QbeastIntegrationTestSpec {

  behavior of "SparkRevisionBuilderTest"

  it should "detect the correct types in getColumnQType" in withSpark(spark => {

    import spark.implicits._
    val df = 0.to(10).map(i => Test(i, i * 2.0, s"$i", i * 1.2f)).toDF()

    SparkRevisionBuilder.getColumnQType("a", df) shouldBe IntegerDataType
    SparkRevisionBuilder.getColumnQType("b", df) shouldBe DoubleDataType
    SparkRevisionBuilder.getColumnQType("c", df) shouldBe StringDataType
    SparkRevisionBuilder.getColumnQType("d", df) shouldBe FloatDataType

  })

  it should "should extract correctly the type" in {

    import SparkRevisionBuilder.SpecExtractor

    "column:LinearTransformer" match {
      case SpecExtractor(column, transformer) =>
        assert(column == "column")
        assert(transformer == "LinearTransformer")
      case _ => fail("It did not recognize the type")
    }

    "column" match {
      case SpecExtractor(column, transformer) =>
        fail("It shouldn't be here")
      case column =>
        assert(column == "column")
    }
  }

  it should "createNewRevision with only one columns" in withSpark(spark => {
    import spark.implicits._
    val df = 0.to(10).map(i => Test(i, i * 2.0, s"$i", i * 1.2f)).toDF()
    val qid = QTableID("t")
    val revision =
      SparkRevisionBuilder.createNewRevision(
        qid,
        df,
        Map(QbeastOptions.COLUMNS_TO_INDEX -> "a", QbeastOptions.CUBE_SIZE -> "10"))

    revision.tableID shouldBe qid
    revision.revisionID shouldBe 0
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(LinearTransformer("a", IntegerDataType))
    revision.transformations shouldBe Vector.empty

  })
  it should "createNewRevision with only indexed columns and no spec" in withSpark(spark => {
    import spark.implicits._
    val df = 0.to(10).map(i => Test(i, i * 2.0, s"$i", i * 1.2f)).toDF()
    val qid = QTableID("t")
    val revision =
      SparkRevisionBuilder.createNewRevision(
        qid,
        df,
        Map(QbeastOptions.COLUMNS_TO_INDEX -> "a,b,c,d", QbeastOptions.CUBE_SIZE -> "10"))

    revision.tableID shouldBe qid
    revision.revisionID shouldBe 0
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(
      LinearTransformer("a", IntegerDataType),
      LinearTransformer("b", DoubleDataType),
      HashTransformer("c", StringDataType),
      LinearTransformer("d", FloatDataType))
    revision.transformations shouldBe Vector.empty

    val revisionExplicit =
      SparkRevisionBuilder.createNewRevision(
        qid,
        df,
        Map(
          QbeastOptions.COLUMNS_TO_INDEX -> "a:linear,b:linear,c:hashing,d:linear",
          QbeastOptions.CUBE_SIZE -> "10"))

    revisionExplicit.copy(timestamp = 0) shouldBe revision.copy(timestamp = 0)
  })

  it should "createNewRevision with only indexed columns with all hash" in withSpark(spark => {
    import spark.implicits._
    val df = 0.to(10).map(i => Test(i, i * 2.0, s"$i", i * 1.2f)).toDF()
    val qid = QTableID("t")
    val revision =
      SparkRevisionBuilder.createNewRevision(
        qid,
        df,
        Map(
          QbeastOptions.COLUMNS_TO_INDEX -> "a:hashing,b:hashing,c:hashing,d:hashing",
          QbeastOptions.CUBE_SIZE -> "10"))

    revision.tableID shouldBe qid
    revision.revisionID shouldBe 0
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(
      HashTransformer("a", IntegerDataType),
      HashTransformer("b", DoubleDataType),
      HashTransformer("c", StringDataType),
      HashTransformer("d", FloatDataType))
    revision.transformations shouldBe Vector.empty

  })
}
