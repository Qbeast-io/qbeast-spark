/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.delta

import io.delta.tables._
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/**
 * Tests for ensuring compatibility between Qbeast and underlying features of Delta Lake
 */
class QbeastDeltaIntegrationTest extends QbeastIntegrationTestSpec {

  def createSimpleTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
  }

  "Qbeast" should "output correctly Operation Metrics in Delta History" in
    withQbeastContextSparkAndTmpDir((spark, tmpDir) => {

      val data = createSimpleTestData(spark)
      data.write
        .format("qbeast")
        .option("columnsToIndex", "a,b")
        .save(tmpDir + "/qbeast")

      val qbeastHistory =
        spark.sql(s"DESCRIBE HISTORY '$tmpDir/qbeast'").select("operationMetrics")

      val historyMap = qbeastHistory.first().get(0).asInstanceOf[Map[String, String]]
      historyMap.size should be > 0
      historyMap.get("numFiles") shouldBe Some("1")
      historyMap.get("numOutputRows") shouldBe Some("3")
      historyMap.get("numOutputBytes") shouldBe Some("660")

    })

  it should "output correctly File Metrics in Commit Log" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {

      val data = createSimpleTestData(spark)
      data.write
        .format("qbeast")
        .option("columnsToIndex", "a,b")
        .save(tmpDir)

      val stats =
        DeltaLog.forTable(spark, tmpDir).unsafeVolatileSnapshot.allFiles.collect().map(_.stats)
      stats.length shouldBe >(0)
      stats.head shouldBe "{\"numRecords\":3,\"minValues\":{\"a\":\"A\",\"b\":1}," +
        "\"maxValues\":{\"a\":\"C\",\"b\":3}," +
        "\"nullCount\":{\"a\":0,\"b\":0}}"

    })

  it should "not write stats when specified" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.databricks.delta.stats.collect", "false"))(
    (spark, tmpDir) => {

      val data = createSimpleTestData(spark)
      data.write
        .format("qbeast")
        .option("columnsToIndex", "a,b")
        .save(tmpDir)

      val stats =
        DeltaLog.forTable(spark, tmpDir).update().allFiles.collect().map(_.stats)
      stats.length shouldBe >(0)
      stats.head shouldBe null

    })

  it should "store userMetadata" in withQbeastContextSparkAndTmpDir((spark, tmpDir) => {
    val data = createSimpleTestData(spark)
    data.write
      .format("qbeast")
      .option("columnsToIndex", "a,b")
      .option("userMetadata", "userMetadata1")
      .save(tmpDir)

    data.write
      .mode("append")
      .format("qbeast")
      .option("userMetadata", "userMetadata2")
      .save(tmpDir)

    import spark.implicits._

    val deltaTable = DeltaTable.forPath(spark, tmpDir)
    val allUserMetadata = deltaTable.history().select("userMetadata").as[String].collect()
    allUserMetadata should contain theSameElementsAs "userMetadata1" :: "userMetadata2" :: Nil
  })

  it should "write userMetadata" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog
      .set("spark.qbeast.index.stagingSizeInBytes", "1")) { (spark, tmpDir) =>
    {
      val df = createSimpleTestData(spark)
      df.write
        .format("qbeast")
        .option("columnsToIndex", "a,b")
        .option("cubeSize", "2000")
        .option("userMetadata", "userMetadata1")
        .save(tmpDir)

      val deltaTable = DeltaTable.forPath(spark, tmpDir)
      val userMetadata =
        deltaTable.history().orderBy("timestamp").select("userMetadata").first().getAs[String](0)

      userMetadata shouldBe "userMetadata1"

    }
  }

}
