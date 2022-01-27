package io.qbeast.core.transform

import io.qbeast.core.model.IntegerDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformerTest extends AnyFlatSpec with Matchers {

  behavior of "Transformer"

  it should "return correct column name" in {
    val columnName = "a"
    val dataType = IntegerDataType
    val transformer = Transformer(columnName, dataType)
    transformer.columnName shouldBe columnName
  }

  it should "return correct spec" in {
    val columnName = "a"
    val dataType = IntegerDataType
    Transformer(columnName, dataType).spec shouldBe "a:linear"
    Transformer("linear", columnName, dataType).spec shouldBe "a:linear"
    Transformer("hashing", columnName, dataType).spec shouldBe "a:hashing"
    an[NoSuchElementException] should be thrownBy Transformer(
      "another",
      columnName,
      dataType).spec

  }

  it should "makeTransformation" in {
    val columnName = "a"
    val dataType = IntegerDataType
    val transformer = Transformer("linear", columnName, "1", dataType)

    val transformation = ColumnStats(min = 0, max = 2, 0, 0.0, Nil)
    transformer
      .makeTransformation(transformation) shouldBe LinearTransformation(0, 2, 1, dataType)
  }

  it should "return new transformation on maybeUpdateTransformation" in {
    val columnName = "a"
    val dataType = IntegerDataType
    val transformer = Transformer("linear", columnName, "4", dataType)

    val transformation = ColumnStats(min = 0, max = 5, 0, 0.0, Nil)
    val currentTransformation = transformer.makeTransformation(transformation)

    val newTransformation = ColumnStats(min = 3, max = 8, 0, 0.0, Nil)
    transformer.maybeUpdateTransformation(currentTransformation, newTransformation) shouldBe Some(
      LinearTransformation(0, 8, 4, dataType))

    transformer.maybeUpdateTransformation(currentTransformation, transformation) shouldBe None
  }
}
