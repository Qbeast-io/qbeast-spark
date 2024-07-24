package io.qbeast.core.model

import io.qbeast.core.transform.HashTransformer
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.core.transform.StringHistogramTransformer
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ColumnToIndexTest extends AnyFlatSpec with Matchers {

  behavior of "ColumnsToIndexTest"

  it should "extract the correct column name" in {

    val columnToIndex = ColumnToIndex("column:linear")
    columnToIndex.columnName shouldBe "column"
    columnToIndex.transformerType shouldBe Some("linear")

  }

  it should "return the transformer type as None" in {

    val columnToIndex = ColumnToIndex("column")
    columnToIndex.columnName shouldBe "column"
    columnToIndex.transformerType shouldBe None

  }

  it should "return the correct Transformer" in {

    val schema = StructType(
      StructField("a", LongType) :: StructField("b", DoubleType) :: StructField(
        "c",
        StringType) :: StructField("d", FloatType) :: Nil)

    val columnToIndex = ColumnToIndex("a")
    val transformer = columnToIndex.toTransformer(schema)
    transformer shouldBe a[LinearTransformer]

  }

  it should "return the correct Transformer with transformer type" in {

    val schema = StructType(
      StructField("a", LongType) :: StructField("b", DoubleType) :: StructField(
        "c",
        StringType) :: StructField("d", FloatType) :: Nil)

    val columnToIndex = ColumnToIndex("a:linear")
    val transformer = columnToIndex.toTransformer(schema)
    transformer shouldBe a[LinearTransformer]

    val columnToIndexHash = ColumnToIndex("a:hashing")
    val transformerString = columnToIndexHash.toTransformer(schema)
    transformerString shouldBe a[HashTransformer]

    // Only StringHistogramTransformer is implemented
    val columnToIndexHistogram = ColumnToIndex("c:histogram")
    val transformerHistogram = columnToIndexHistogram.toTransformer(schema)
    transformerHistogram shouldBe a[StringHistogramTransformer]

  }

}
