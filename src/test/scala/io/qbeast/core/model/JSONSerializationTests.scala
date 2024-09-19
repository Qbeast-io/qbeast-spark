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
package io.qbeast.core.model

import io.qbeast.core.transform.CDFNumericQuantilesTransformation
import io.qbeast.core.transform.CDFStringQuantilesTransformation
import io.qbeast.core.transform.HashTransformation
import io.qbeast.core.transform.LinearTransformation
import io.qbeast.core.transform.Transformation
import io.qbeast.core.transform.Transformer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date
import java.sql.Timestamp
import scala.util.hashing.MurmurHash3
import scala.util.Random

class JSONSerializationTests extends AnyFlatSpec with Matchers {

  "QTableID" should "have a small json representation serializable" in {
    val tableID = new QTableID("test")
    mapper.writeValueAsString(tableID) shouldBe "\"test\""
    mapper.readValue[QTableID]("\"test\"", classOf[QTableID]) shouldBe tableID
  }

  "A QDataType" should "just serialize with its name" in {
    for (dataName <- QDataType.qtypes.keys) {
      val dataType = QDataType(dataName)
      val j = s""""$dataName""""
      mapper.writeValueAsString(dataType) shouldBe j
      mapper.readValue[QDataType](j, classOf[QDataType]) shouldBe dataType
    }

  }

  "A transformer" should "Serializer with the class type" in {
    val tr: Transformer = Transformer("linear", "test1", DoubleDataType)
    val ser =
      """{"className":"io.qbeast.core.transform.LinearTransformer",""" +
        """"columnName":"test1","dataType":"DoubleDataType"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformer](ser, classOf[Transformer]) shouldBe tr

    val tr2: Transformer = Transformer("linear", "test1", IntegerDataType)
    val ser2 =
      """{"className":"io.qbeast.core.transform.LinearTransformer",""" +
        """"columnName":"test1","dataType":"IntegerDataType"}"""
    mapper.writeValueAsString(tr2) shouldBe ser2
    mapper.readValue[Transformer](ser2, classOf[Transformer]) shouldBe tr2
  }

  "A LinearTransformation" should "serializer with the class type con Double" in {
    val tr: Transformation = LinearTransformation(0.0, 10.0, 5.0, DoubleDataType)
    val ser =
      """{"className":"io.qbeast.core.transform.LinearTransformation",""" +
        """"minNumber":0.0,"maxNumber":10.0,"nullValue":5.0,"orderedDataType":"DoubleDataType"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }

  "A LinearTransformation" should "serializer with the class type con Integer" in {
    val tr: Transformation = LinearTransformation(0, 10, 5, IntegerDataType)
    val ser =
      """{"className":"io.qbeast.core.transform.LinearTransformation",""" +
        """"minNumber":0,"maxNumber":10,"nullValue":5,"orderedDataType":"IntegerDataType"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }

  "A LinearTransformation" should "serializer with the class type con Long" in {
    val tr: Transformation = LinearTransformation(0L, 10L, 5L, LongDataType)
    val ser =
      """{"className":"io.qbeast.core.transform.LinearTransformation",""" +
        """"minNumber":0,"maxNumber":10,"nullValue":5,"orderedDataType":"LongDataType"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }

  "A LinearTransformation" should "serializer with the class type con Timestamp" in {
    val minTimestamp = Timestamp.valueOf("2017-01-01 12:02:00").getTime
    val maxTimestamp = Timestamp.valueOf("2017-01-02 12:02:00").getTime
    val nullTimestamp = Timestamp.valueOf("2017-01-01 18:02:00").getTime
    val tr: Transformation =
      LinearTransformation(minTimestamp, maxTimestamp, nullTimestamp, TimestampDataType)
    val ser =
      """{"className":"io.qbeast.core.transform.LinearTransformation",""" +
        s""""minNumber":$minTimestamp,"maxNumber":$maxTimestamp,""" +
        s""""nullValue":$nullTimestamp,"orderedDataType":"TimestampDataType"}""".stripMargin
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }

  "A LinearTransformation" should "serializer with the class type con Date" in {
    val minTimestamp = Date.valueOf("2017-01-01").getTime
    val maxTimestamp = Date.valueOf("2017-01-02").getTime
    val nullTimestamp = Date.valueOf("2017-01-01").getTime
    val tr: Transformation =
      LinearTransformation(minTimestamp, maxTimestamp, nullTimestamp, DateDataType)
    val ser =
      """{"className":"io.qbeast.core.transform.LinearTransformation",""" +
        s""""minNumber":$minTimestamp,"maxNumber":$maxTimestamp,""" +
        s""""nullValue":$nullTimestamp,"orderedDataType":"DateDataType"}""".stripMargin
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }

  it should "deserialize old LinearTransformation with the same random null value" in {
    val min = 0
    val max = 10
    val ser =
      """{"className":"io.qbeast.core.transform.LinearTransformation",""" +
        """"minNumber":0,"maxNumber":10,"orderedDataType":"IntegerDataType"}"""
    val tree = """{"minNumber":0,"maxNumber":10,"orderedDataType":"IntegerDataType"}"""

    val hash = MurmurHash3.stringHash(tree)
    val random = new Random(hash).nextDouble()
    val nullValue = 0 + (random * (10 - 0)).toInt

    val tr: Transformation = LinearTransformation(min, max, nullValue, IntegerDataType)

    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr
  }

  "A HashTransformation" should "serialize correctly" in {
    val tr: Transformation = HashTransformation("null")
    val ser =
      """{"className":"io.qbeast.core.transform.HashTransformation","nullValue":"null"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }

  "A revision" should "Serialize with all simple value a part from Transform*" in {
    val rev =
      Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(Transformer("linear", "test1", DoubleDataType)),
        List(LinearTransformation(0.0, 10.0, 5.0, DoubleDataType)))
    val json =
      """{"revisionID":12,"timestamp":12,"tableID":"test","desiredCubeSize":100,""" +
        """"columnTransformers":[{"className":"io.qbeast.core.transform.LinearTransformer",""" +
        """"columnName":"test1","dataType":"DoubleDataType"}],"transformations":""" +
        """[{"className":"io.qbeast.core.transform.LinearTransformation","minNumber":0.0,""" +
        """"maxNumber":10.0,"nullValue":5.0,"orderedDataType":"DoubleDataType"}]}"""
    mapper.writeValueAsString(rev) shouldBe json
    mapper.readValue[Revision](json, classOf[Revision]) shouldBe rev

  }

  "A revision" should "Serialize with all Long Transform*" in {
    val rev =
      Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(Transformer("linear", "test1", LongDataType)),
        List(LinearTransformation(0L, 100L, 5L, LongDataType)))
    val json =
      """{"revisionID":12,"timestamp":12,"tableID":"test","desiredCubeSize":100,""" +
        """"columnTransformers":[{"className":"io.qbeast.core.transform.LinearTransformer",""" +
        """"columnName":"test1","dataType":"LongDataType"}],"transformations":""" +
        """[{"className":"io.qbeast.core.transform.LinearTransformation","minNumber":0,""" +
        """"maxNumber":100,"nullValue":5,"orderedDataType":"LongDataType"}]}"""
    mapper.writeValueAsString(rev) shouldBe json
    mapper.readValue[Revision](json, classOf[Revision]) shouldBe rev

  }

  "A revision" should "Serialize with all Hash Transform*" in {
    val rev =
      Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        List(Transformer("hashing", "test1", StringDataType)),
        List(HashTransformation("null")))
    val json =
      """{"revisionID":12,"timestamp":12,"tableID":"test","desiredCubeSize":100,""" +
        """"columnTransformers":[{"className":"io.qbeast.core.transform.HashTransformer",""" +
        """"columnName":"test1","dataType":"StringDataType"}],"transformations":""" +
        """[{"className":"io.qbeast.core.transform.HashTransformation","nullValue":"null"}]}"""
    mapper.writeValueAsString(rev) shouldBe json
    mapper.readValue[Revision](json, classOf[Revision]) shouldBe rev

  }

  "A CDFTransformation" should "be serialized according to its type" in {
    val tr: Transformation =
      CDFNumericQuantilesTransformation(IndexedSeq(1, 2, 3), IntegerDataType)
    val ser =
      """{"className":"io.qbeast.core.transform.CDFNumericQuantilesTransformation",
        |"quantiles":[1.0,2.0,3.0],"dataType":"IntegerDataType"}""".stripMargin

    println(mapper.writeValueAsString(tr))
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue(ser, classOf[Transformation]) shouldBe tr

    val tr2: Transformation =
      CDFStringQuantilesTransformation(IndexedSeq("a", "b", "c"))
    val ser2 =
      """{"className":"io.qbeast.core.transform.CDFStringQuantilesTransformation",
        |"quantiles":["a","b","c"]}""".stripMargin
    println(mapper.writeValueAsString(tr2))
    mapper.writeValueAsString(tr2) shouldBe ser2
    mapper.readValue(ser2, classOf[Transformation]) shouldBe tr2

  }

}
