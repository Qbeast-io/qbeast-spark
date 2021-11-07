package io.qbeast.model

import io.qbeast.transform.{HashTransformation, LinearTransformation, Transformation, Transformer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
      """{"className":"io.qbeast.transform.LinearTransformer",""" +
        """"columnName":"test1","dataType":"DoubleDataType"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformer](ser, classOf[Transformer]) shouldBe tr

    val tr2: Transformer = Transformer("linear", "test1", IntegerDataType)
    val ser2 =
      """{"className":"io.qbeast.transform.LinearTransformer",""" +
        """"columnName":"test1","dataType":"IntegerDataType"}"""
    mapper.writeValueAsString(tr2) shouldBe ser2
    mapper.readValue[Transformer](ser2, classOf[Transformer]) shouldBe tr2
  }

  "A LinearTransformation" should "serializer with the class type con Double" in {
    val tr: Transformation = LinearTransformation(0.0, 10.0, DoubleDataType)
    val ser =
      """{"className":"io.qbeast.transform.LinearTransformation",""" +
        """"minNumber":0.0,"maxNumber":10.0,"orderedDataType":"DoubleDataType"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }
  "A LinearTransformation" should "serializer with the class type con Integer" in {
    val tr: Transformation = LinearTransformation(0, 10, IntegerDataType)
    val ser =
      """{"className":"io.qbeast.transform.LinearTransformation",""" +
        """"minNumber":0,"maxNumber":10,"orderedDataType":"IntegerDataType"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }
  "A LinearTransformation" should "serializer with the class type con Long" in {
    val tr: Transformation = LinearTransformation(0L, 10L, LongDataType)
    val ser =
      """{"className":"io.qbeast.transform.LinearTransformation",""" +
        """"minNumber":0,"maxNumber":10,"orderedDataType":"LongDataType"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }

  "A HashTransformation" should "serialize correctly" in {
    val tr: Transformation = HashTransformation()
    val ser =
      """{"className":"io.qbeast.transform.HashTransformation"}"""
    mapper.writeValueAsString(tr) shouldBe ser
    mapper.readValue[Transformation](ser, classOf[Transformation]) shouldBe tr

  }

  "A revision" should "Serialize with all simple value a part from Transoform*" in {
    val rev =
      Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(Transformer("linear", "test1", DoubleDataType)),
        List(LinearTransformation(0.0, 10.0, DoubleDataType)))
    val json =
      """{"revisionID":12,"timestamp":12,"tableID":"test","desiredCubeSize":100,""" +
        """"columnTransformers":[{"className":"io.qbeast.transform.LinearTransformer",""" +
        """"columnName":"test1","dataType":"DoubleDataType"}],"transformations":""" +
        """[{"className":"io.qbeast.transform.LinearTransformation","minNumber":0.0,""" +
        """"maxNumber":10.0,"orderedDataType":"DoubleDataType"}]}"""
    mapper.writeValueAsString(rev) shouldBe json
    mapper.readValue[Revision](json, classOf[Revision]) shouldBe rev

  }
  "A revision" should "Serialize with all Long Transoform*" in {
    val rev =
      Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        Vector(Transformer("linear", "test1", LongDataType)),
        List(LinearTransformation(0L, 100L, LongDataType)))
    val json =
      """{"revisionID":12,"timestamp":12,"tableID":"test","desiredCubeSize":100,""" +
        """"columnTransformers":[{"className":"io.qbeast.transform.LinearTransformer",""" +
        """"columnName":"test1","dataType":"LongDataType"}],"transformations":""" +
        """[{"className":"io.qbeast.transform.LinearTransformation","minNumber":0,""" +
        """"maxNumber":100,"orderedDataType":"LongDataType"}]}"""
    mapper.writeValueAsString(rev) shouldBe json
    mapper.readValue[Revision](json, classOf[Revision]) shouldBe rev

  }
  "A revision" should "Serialize with all Hash Transoform*" in {
    val rev =
      Revision(
        12L,
        12L,
        QTableID("test"),
        100,
        List(Transformer("hashing", "test1", StringDataType)),
        List(HashTransformation()))
    val json =
      """{"revisionID":12,"timestamp":12,"tableID":"test","desiredCubeSize":100,""" +
        """"columnTransformers":[{"className":"io.qbeast.transform.HashTransformer",""" +
        """"columnName":"test1","dataType":"StringDataType"}],"transformations":""" +
        """[{"className":"io.qbeast.transform.HashTransformation"}]}"""
    mapper.writeValueAsString(rev) shouldBe json
    mapper.readValue[Revision](json, classOf[Revision]) shouldBe rev

  }

}
