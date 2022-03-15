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
    val transformer = Transformer(columnName, dataType)

    val transformation = Map("a_min" -> 0, "a_max" -> 1)
    transformer
      .makeTransformation(transformation) should matchPattern {
      case LinearTransformation(0, 1, _, IntegerDataType) =>
    }
  }

  it should "return new transformation on maybeUpdateTransformation" in {
    val columnName = "a"
    val dataType = IntegerDataType
    val transformer = Transformer(columnName, dataType)

    val transformation = Map("a_min" -> 0, "a_max" -> 1)
    val currentTransformation = transformer.makeTransformation(transformation)

    val newTransformation = Map("a_min" -> 3, "a_max" -> 8)
    transformer.maybeUpdateTransformation(
      currentTransformation,
      newTransformation) should matchPattern {
      case Some(LinearTransformation(0, 8, _, IntegerDataType)) =>
    }

    transformer.maybeUpdateTransformation(currentTransformation, transformation) shouldBe None
  }
}
