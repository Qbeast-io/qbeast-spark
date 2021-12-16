package io.qbeast.context

import org.apache.spark.SparkConf
import org.apache.spark.qbeast.config
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastConfigTest extends AnyFlatSpec with Matchers {
  "Qbeast config" should "use default configurations" in withSpark(new SparkConf()) {
    config.DEFAULT_CUBE_SIZE shouldBe 100000
    config.MIN_PARTITION_CUBE_SIZE shouldBe 100
  }

  it should "change configurations accordingly" in {
    val sparkConfig = new SparkConf()
      .set("spark.qbeast.index.defaultCubeSize", "1000")
      .set("spark.qbeast.index.minPartitionCubeSize", "1000")
    withSpark(sparkConfig) {
      config.DEFAULT_CUBE_SIZE shouldBe 1000
      config.MIN_PARTITION_CUBE_SIZE shouldBe 1000
    }
  }

  "Spark.qbeast.keeper" should "not be defined" in withSpark(new SparkConf()) {
    val keeperString = SparkSession.active.sparkContext.getConf
      .getOption("spark.qbeast.keeper")
    keeperString shouldBe None
  }

  it should "be defined" in {
    val sparkConfig = new SparkConf()
      .set("spark.qbeast.keeper", "myKeeper")
    withSpark(sparkConfig) {
      val keeperString = SparkSession.active.sparkContext.getConf
        .getOption("spark.qbeast.keeper")
      keeperString.get shouldBe "myKeeper"
    }
  }

  private def withSpark[T](conf: SparkConf)(code: => T): T = {
    val session = SparkSession.builder().master("local[*]").config(conf).getOrCreate()
    try {
      code
    } finally {
      session.close()
    }
  }

}
