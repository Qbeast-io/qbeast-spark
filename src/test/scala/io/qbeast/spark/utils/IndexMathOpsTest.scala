package io.qbeast.spark.utils

import io.qbeast.core.model.CubeId
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.scalatest.matchers.should.Matchers

class IndexMathOpsTest extends QbeastIntegrationTestSpec with Matchers {

  behavior of "IndexMathOpsTest.averageFanout"

  it should "return  the right avg fanout with Set" in {
    val cubes =
      Set(CubeId(3, "1"), CubeId(3, "11"), CubeId(3, "12"), CubeId(3, "13"), CubeId(3, "2"))
    IndexMathOps.averageFanout(cubes) shouldBe 3.0

  }

  it should "return  the right avg fanout with Dataset[CubeId]" in withSpark { (spark) =>
    import spark.implicits._
    val cubes =
      Seq(CubeId(3, "1"), CubeId(3, "11"), CubeId(3, "12"), CubeId(3, "13"), CubeId(3, "2")).toDS
    IndexMathOps.averageFanout(cubes) shouldBe 3.0
  }

}
