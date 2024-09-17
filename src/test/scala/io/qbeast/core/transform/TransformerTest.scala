/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.core.transform

import io.qbeast.core.model.DateDataType
import io.qbeast.core.model.IntegerDataType
import io.qbeast.core.model.StringDataType
import io.qbeast.core.model.TimestampDataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.sql.Timestamp

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

    an[Exception] should be thrownBy Transformer("histogram", columnName, dataType).spec

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

  it should "makeTransformation with String histograms" in {
    val columnName = "s"
    val dataType = StringDataType
    val transformer = Transformer("histogram", columnName, dataType)
    transformer shouldBe a[CDFStringQuantilesTransformer]

    val quantiles = Seq("str_1", "str_2", "str_3", "str_4", "str_5", "str_6")
    val transformation = Map(s"${columnName}_quantiles" -> quantiles)
    transformer.makeTransformation(transformation) match {
      case _ @CDFQuantilesTransformation(quantilesT, _) =>
        quantiles == quantilesT shouldBe true
      case _ => fail("should always be StringHistogramTransformation")
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

  "An EmptyTransformer" should "create an EmptyTransformation without stats" in {
    EmptyTransformer.transformerSimpleName shouldBe "empty"

    val colName = "a"
    val transformer = EmptyTransformer(colName, StringDataType)

    val transformation = transformer.makeTransformation(r => r)
    transformation shouldBe a[EmptyTransformation]
  }

}
