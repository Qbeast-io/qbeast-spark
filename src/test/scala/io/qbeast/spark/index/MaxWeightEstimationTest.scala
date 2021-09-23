package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.{col, udaf}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class MaxWeightEstimationTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {
  "MaxWeight merge" should "compute fraction correctly" in {
    val weightA = NormalizedWeight(Weight.MaxValue)
    val weightB = NormalizedWeight(Weight.MaxValue)
    MaxWeightEstimation.merge(weightA, weightB) shouldBe 0.5
  }

  "MaxWeight reduce" should "compute fraction correctly" in {
    val weightA = NormalizedWeight(Weight.MaxValue)
    val weightB = NormalizedWeight(Weight.MaxValue)
    MaxWeightEstimation.reduce(weightA, weightB) shouldBe 0.5
  }

  "MaxWeight finish" should "return same fraction" in {
    val finalWeight = NormalizedWeight(Weight(Random.nextInt()))
    MaxWeightEstimation.finish(finalWeight) shouldBe finalWeight
  }

  "MaxWeight zero" should "be minium positive value" in {
    MaxWeightEstimation.zero shouldBe 0.0
  }

  "MaxWeight" should "merge weights correctly on DataFrame" in withSpark { spark =>
    import spark.implicits._
    val weightA = 1.0
    val weightB = 1.0
    val weightC = 0.5

    val df = Seq(weightA, weightB, weightC).toDF("weight")
    val maxWeight = udaf(MaxWeightEstimation)
    df.agg(maxWeight(col("weight"))).first().getDouble(0) shouldBe 0.25
  }

}
