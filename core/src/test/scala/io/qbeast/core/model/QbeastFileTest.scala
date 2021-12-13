package io.qbeast.core.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastFileTest extends AnyFlatSpec with Matchers {
  "QbeastFile" should "find all the keys in the map" in {
    val fileMetadata: Map[String, String] = Map(
      "cube" -> CubeId.root(1).string,
      "minWeight" -> "19217",
      "maxWeight" -> "11111111",
      "state" -> "FlOODED",
      "revision" -> "1",
      "elementCount" -> "777")

    val qbeastFile = QbeastFile("path", fileMetadata)
    qbeastFile.cube shouldBe CubeId.root(1).string
    qbeastFile.minWeight shouldBe Weight(19217)
    qbeastFile.maxWeight shouldBe Weight(11111111)
    qbeastFile.state shouldBe "FlOODED"
    qbeastFile.revision shouldBe 1
    qbeastFile.elementCount shouldBe 777
  }

  it should "throw exception if key not found" in {
    val fileMetadata = Map.empty[String, String]
    a[IllegalArgumentException] shouldBe thrownBy(QbeastFile("path", fileMetadata))
  }

  it should "throw error if the types are different" in {
    val fileMetadata: Map[String, String] = Map(
      "cube" -> CubeId.root(1).string,
      "minWeight" -> "19217",
      "maxWeight" -> "11111111",
      "state" -> "FlOODED",
      "revision" -> "bad_type",
      "elementCount" -> "777")

    a[IllegalArgumentException] shouldBe thrownBy(QbeastFile("path", fileMetadata))
  }
}
