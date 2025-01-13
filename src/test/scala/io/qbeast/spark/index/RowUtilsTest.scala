package io.qbeast.spark.index

import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.model.Point
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.core.model.StringDataType
import io.qbeast.core.transform.CDFNumericQuantilesTransformation
import io.qbeast.core.transform.CDFQuantilesTransformer
import io.qbeast.core.transform.HashTransformation
import io.qbeast.core.transform.HashTransformer
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.AnalysisException

class RowUtilsTest extends QbeastIntegrationTestSpec {

  "RowUtils" should "transform a row using LinearTransformation" in withQbeastContextSparkAndTmpWarehouse {
    (spark, _) =>
      import spark.implicits._
      val df = Seq((1.0, "a")).toDF("id", "name")
      val revision = Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(LinearTransformer("id", DoubleDataType)),
        List(LinearTransformation(0.0, 10.0, 5.0, DoubleDataType)))
      val row = df.head
      val transformedRow = RowUtils.rowValuesToPoint(row, revision)
      transformedRow shouldBe Point(Vector(0.1))

  }

  it should "transform a row using HashTransformation" in withQbeastContextSparkAndTmpWarehouse {
    (spark, _) =>
      import spark.implicits._
      val df = Seq(("1", "a")).toDF("id", "name")
      val revision = Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(HashTransformer("id", StringDataType)),
        List(HashTransformation("null")))
      val row = df.head
      val transformedRow = RowUtils.rowValuesToPoint(row, revision)
      transformedRow shouldBe Point(Vector(0.24913018394686756))

  }

  it should "transform a row using quantiles" in withQbeastContextSparkAndTmpWarehouse {
    (spark, _) =>
      import spark.implicits._
      val df = Seq((1.0, "a")).toDF("id", "name")
      val revision = Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(CDFQuantilesTransformer("id", DoubleDataType)),
        List(CDFNumericQuantilesTransformation(Array(0.0, 2.0), DoubleDataType)))
      val row = df.head
      val transformedRow = RowUtils.rowValuesToPoint(row, revision)
      transformedRow shouldBe Point(Vector(0.5))
  }

  it should "throw an error when values are out of max bound" in withQbeastContextSparkAndTmpWarehouse {
    (spark, _) =>
      import spark.implicits._
      val df = Seq((20.0, "a")).toDF("id", "name")
      val revision = Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(LinearTransformer("id", DoubleDataType)),
        List(LinearTransformation(0.0, 10.0, 5.0, DoubleDataType)))
      val row = df.head
      an[AssertionError] shouldBe thrownBy(RowUtils.rowValuesToPoint(row, revision))

  }

  it should "throw an error when values are out of min bound" in withQbeastContextSparkAndTmpWarehouse {
    (spark, _) =>
      import spark.implicits._
      val df = Seq((-1.0, "a")).toDF("id", "name")
      val revision = Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(LinearTransformer("id", DoubleDataType)),
        List(LinearTransformation(0.0, 10.0, 5.0, DoubleDataType)))
      val row = df.head
      an[AssertionError] shouldBe thrownBy(RowUtils.rowValuesToPoint(row, revision))

  }

  it should "throw an error when Transformations are empty" in withQbeastContextSparkAndTmpWarehouse {
    (spark, _) =>
      import spark.implicits._
      val df = Seq((-1.0, "a")).toDF("id", "name")
      val revision = Revision(12L, 12L, QTableID("test"), 100, Vector.empty, List.empty)
      val row = df.head
      an[AnalysisException] shouldBe thrownBy(RowUtils.rowValuesToPoint(row, revision))

  }

}
