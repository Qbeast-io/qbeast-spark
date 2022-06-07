package io.qbeast.context

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.SparkConf
import org.apache.spark.qbeast.config
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastConfigTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  "Qbeast config" should "use default configurations" in withSpark { _ =>
    config.DEFAULT_CUBE_SIZE shouldBe 5000000
    config.CUBE_WEIGHTS_BUFFER_CAPACITY shouldBe 100000
  }

  it should "change configurations accordingly" in withExtendedSpark(
    new SparkConf()
      .set("spark.qbeast.index.defaultCubeSize", "1000")
      .set("spark.qbeast.index.cubeWeightsBufferCapacity", "1000")
      .set("spark.qbeast.index.numberOfRetries", "10")) { _ =>
    config.DEFAULT_CUBE_SIZE shouldBe 1000
    config.CUBE_WEIGHTS_BUFFER_CAPACITY shouldBe 1000
    config.DEFAULT_NUMBER_OF_RETRIES shouldBe 10

  }

  "Spark.qbeast.keeper" should "not be defined" in withSpark { _ =>
    val keeperString = SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.keeper")
    keeperString shouldBe None
  }

  it should "be defined" in withExtendedSpark(
    new SparkConf()
      .set("spark.qbeast.keeper", "myKeeper")) { spark =>
    val keeperString = spark.sparkContext.getConf.getOption("spark.qbeast.keeper")
    keeperString.get shouldBe "myKeeper"

  }

}
