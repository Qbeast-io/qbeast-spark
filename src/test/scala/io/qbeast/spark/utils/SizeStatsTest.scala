package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.col

class SizeStatsTest extends QbeastIntegrationTestSpec {

  def areEqual(thisStats: SizeStats, otherStats: SizeStats): Boolean =
    thisStats.count == otherStats.count &&
      thisStats.avg == otherStats.avg &&
      thisStats.stddev == otherStats.stddev &&
      (thisStats.quartiles sameElements otherStats.quartiles)

  "SizeStatsTest" should "work with longs" in withSpark { spark =>
    import spark.implicits._

    val sizeStats = spark
      .range(1001)
      .select(SizeStats.forColumn(col("id")).as("stats"))
      .select("stats.*")
      .as[SizeStats]
      .first()

    sizeStats.count shouldBe 1001
    sizeStats.avg shouldBe 500
    sizeStats.stddev shouldBe 289
    sizeStats.quartiles sameElements Array(0, 250, 500, 750, 1000) shouldBe true
  }

}
