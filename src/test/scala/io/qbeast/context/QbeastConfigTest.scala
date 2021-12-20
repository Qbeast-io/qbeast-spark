package io.qbeast.context

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.SparkConf
import org.apache.spark.qbeast.config
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastConfigTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  "Qbeast config" should "use default configurations" in withSpark { spark =>
    config.DEFAULT_CUBE_SIZE shouldBe 100000
    config.MIN_PARTITION_CUBE_SIZE shouldBe 100
  }

  it should "change configurations accordingly" in withExtendedSpark(
    new SparkConf()
      .set("spark.qbeast.index.defaultCubeSize", "1000")
      .set("spark.qbeast.index.minPartitionCubeSize", "1000")) { spark =>
    config.DEFAULT_CUBE_SIZE shouldBe 1000
    config.MIN_PARTITION_CUBE_SIZE shouldBe 1000
  }

  "Spark.qbeast.keeper" should "not be defined" in withSpark { spark =>
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
