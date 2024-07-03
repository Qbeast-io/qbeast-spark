package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.scalatest.matchers.should.Matchers

class IndexMathOpsTest extends QbeastIntegrationTestSpec with Matchers {

  "IndexMathOps.averageFanout" should "return the right avg fanout with Set" in {
//    val cubes =
//      Set(CubeId(3, "1"), CubeId(3, "11"), CubeId(3, "12"), CubeId(3, "13"), CubeId(3, "2"))
//    IndexMathOps.averageFanout(cubes) shouldBe 3.0
    fail("Not implemented")
  }

  it should "return the right avg fanout with Dataset[CubeId]" in withSpark { (spark) =>
//    import spark.implicits._
//    val cubes =
//      Seq(CubeId(3, "1"), CubeId(3, "11"), CubeId(3, "12"), CubeId(3, "13"), CubeId(3, "2")).toDS
//    IndexMathOps.averageFanout(cubes) shouldBe 3.0
    fail("Not implemented")
  }

  "minHeight" should "return the right min height" in {
    fail("Not implemented")
  }

  "logOfBase" should "return the right log of base" in {
    fail("Not implemented")
  }

  "l1Deviation" should "return the right l1 deviation" in {
    fail("Not implemented")
  }

  "l2Deviation" should "return the right l2 deviation" in {
    fail("Not implemented")
  }

  "stddev" should "return the right standard deviation" in {
    fail("Not implemented")
  }

  "round" should "return the right rounded value" in {
    fail("Not implemented")
  }

}
