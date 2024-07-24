package io.qbeast.core.model

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnToIndexUtilsTest extends AnyFlatSpec with Matchers {

  behavior of "SparkRevisionFactory"

  it should "detect the correct types in getColumnQType" in {

    val schema = StructType(
      StructField("a", LongType) :: StructField("b", DoubleType) :: StructField(
        "c",
        StringType) :: StructField("d", FloatType) :: Nil)

    ColumnToIndexUtils.getColumnQType("a", schema) shouldBe LongDataType
    ColumnToIndexUtils.getColumnQType("b", schema) shouldBe DoubleDataType
    ColumnToIndexUtils.getColumnQType("c", schema) shouldBe StringDataType
    ColumnToIndexUtils.getColumnQType("d", schema) shouldBe FloatDataType

  }

  it should "should extract correctly the type" in {

    import ColumnToIndexUtils.SpecExtractor

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

}
