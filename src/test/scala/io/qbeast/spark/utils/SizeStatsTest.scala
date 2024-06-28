package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.col

class SizeStatsTest extends QbeastIntegrationTestSpec {

  "SizeStatsTest" should "work with longs" in withSpark { (spark) =>
    import spark.implicits._
    val a = spark.range(1000)
    val t = SizeStats.forColumn(col("id"))

    a.select(t).as[SizeStats].first() shouldBe SizeStats(1000, 5000, 288, Array(0.0,250.0,500.0,750.0,1000.0))
  }

}
