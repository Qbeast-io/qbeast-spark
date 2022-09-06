package io.qbeast.core.transform

import io.qbeast.core.model.{DateDataType, IntegerDataType, TimestampDataType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.{Date, Timestamp}

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

  it should "makeTransformation with Timestamp data type" in {
    val columnName = "a"
    val dataType = TimestampDataType
    val transformer = Transformer(columnName, dataType)

    val minTimestamp = Timestamp.valueOf("2017-01-01 12:02:00")
    val maxTimestamp = Timestamp.valueOf("2017-01-03 12:02:00")

    val transformation = Map("a_min" -> minTimestamp, "a_max" -> maxTimestamp)
    val resTransformation =
      transformer.makeTransformation(transformation).asInstanceOf[LinearTransformation]

    resTransformation.minNumber shouldBe minTimestamp.getTime
    resTransformation.maxNumber shouldBe maxTimestamp.getTime
    resTransformation.orderedDataType shouldBe TimestampDataType

  }

  it should "makeTransformation with Date data type" in {
    val columnName = "a"
    val dataType = DateDataType
    val transformer = Transformer(columnName, dataType)

    val minTimestamp = Date.valueOf("2017-01-01")
    val maxTimestamp = Date.valueOf("2017-01-03")

    val transformation = Map("a_min" -> minTimestamp, "a_max" -> maxTimestamp)
    val resTransformation =
      transformer.makeTransformation(transformation).asInstanceOf[LinearTransformation]

    resTransformation.minNumber shouldBe minTimestamp.getTime
    resTransformation.maxNumber shouldBe maxTimestamp.getTime
    resTransformation.orderedDataType shouldBe DateDataType

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
