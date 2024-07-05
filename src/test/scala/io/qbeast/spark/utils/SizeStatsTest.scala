package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.col

class SizeStatsTest extends QbeastIntegrationTestSpec {

  "SizeStatsTest" should "work with longs" in withSpark { spark =>
    import spark.implicits._

    val sizeStats = spark
      .range(1001)
      .select(SizeStats.forColumn(col("id")).as("stats"))
      .select("stats.*")
      .as[SizeStats]
      .first()

    sizeStats shouldBe SizeStats(1001, 500, 289, Seq(0, 250, 500, 750, 1000))
  }

}
