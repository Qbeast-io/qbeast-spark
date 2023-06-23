package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastBlockTest extends AnyFlatSpec with Matchers {
  "QbeastBlock" should "find all the keys in the map" in {
    val blockMetadata: Map[String, String] = Map(
      "minWeight" -> "19217",
      "cube" -> "",
      "maxWeight" -> "11111111",
      "state" -> "FlOODED",
      "replicated" -> "false",
      "revision" -> "1",
      "elementCount" -> "777")

    val qbeastBlock = QbeastBlock("path", blockMetadata, 0L, 0L)
    qbeastBlock.cube shouldBe ""
    qbeastBlock.minWeight shouldBe Weight(19217)
    qbeastBlock.maxWeight shouldBe Weight(11111111)
    qbeastBlock.state shouldBe "FlOODED"
    qbeastBlock.revision shouldBe 1
    qbeastBlock.elementCount shouldBe 777
  }

  it should "throw exception if key not found" in {
    val blockMetadata = Map.empty[String, String]
    a[IllegalArgumentException] shouldBe thrownBy(QbeastBlock("path", blockMetadata, 0L, 0L))
  }

  it should "throw error if the types are different" in {
    val blockMetadata: Map[String, String] = Map(
      "minWeight" -> "19217",
      "cube" -> "",
      "maxWeight" -> "11111111",
      "state" -> "FlOODED",
      "replicated" -> "false",
      "revision" -> "bad_type",
      "elementCount" -> "777")

    a[IllegalArgumentException] shouldBe thrownBy(QbeastBlock("path", blockMetadata, 0L, 0L))
  }
}
