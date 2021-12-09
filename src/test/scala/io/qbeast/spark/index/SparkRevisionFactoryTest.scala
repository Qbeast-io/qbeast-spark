package io.qbeast.spark.index

import io.qbeast.TestClasses.T3
import io.qbeast.core.model._
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.core.transform.{HashTransformer, LinearTransformer}

class SparkRevisionFactoryTest extends QbeastIntegrationTestSpec {

  behavior of "SparkRevisionFactory"

  it should "detect the correct types in getColumnQType" in withSpark(spark => {

    import spark.implicits._
    val df = 0.to(10).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).toDF().schema

    SparkRevisionFactory.getColumnQType("a", df) shouldBe IntegerDataType
    SparkRevisionFactory.getColumnQType("b", df) shouldBe DoubleDataType
    SparkRevisionFactory.getColumnQType("c", df) shouldBe StringDataType
    SparkRevisionFactory.getColumnQType("d", df) shouldBe FloatDataType

  })

  it should "should extract correctly the type" in {

    import SparkRevisionFactory.SpecExtractor

    "column:LinearTransformer" match {
      case SpecExtractor(column, transformer) =>
        column shouldBe "column"
        transformer shouldBe "LinearTransformer"
      case _ => fail("It did not recognize the type")
    }

    "column" match {
      case SpecExtractor(column, transformer) =>
        fail("It shouldn't be here")
      case column =>
        column shouldBe "column"
    }
  }

  it should "createNewRevision with only one columns" in withSpark(spark => {
    import spark.implicits._
    val schema = 0.to(10).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).toDF().schema
    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        Map(QbeastOptions.COLUMNS_TO_INDEX -> "a", QbeastOptions.CUBE_SIZE -> "10"))

    revision.tableID shouldBe qid
    revision.revisionID shouldBe 0
    revision.desiredCubeSize shouldBe 10
    revision.columnTransformers shouldBe Vector(LinearTransformer("a", IntegerDataType))
    revision.transformations shouldBe Vector.empty

  })
  it should "createNewRevision with only indexed columns and no spec" in withSpark(spark => {
    import spark.implicits._
    val schema = 0.to(10).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).toDF().schema
    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
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
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
        Map(
          QbeastOptions.COLUMNS_TO_INDEX -> "a:linear,b:linear,c:hashing,d:linear",
          QbeastOptions.CUBE_SIZE -> "10"))

    revisionExplicit.copy(timestamp = 0) shouldBe revision.copy(timestamp = 0)
  })

  it should "createNewRevision with only indexed columns with all hash" in withSpark(spark => {
    import spark.implicits._
    val schema = 0.to(10).map(i => T3(i, i * 2.0, s"$i", i * 1.2f)).toDF().schema
    val qid = QTableID("t")
    val revision =
      SparkRevisionFactory.createNewRevision(
        qid,
        schema,
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
