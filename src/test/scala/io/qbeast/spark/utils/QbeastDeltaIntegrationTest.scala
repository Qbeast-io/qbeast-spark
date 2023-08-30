package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.{DeltaLog, TimestampNTZTableFeature}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime

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

  // TODO TIMESTAMP NTZ
  it should "index tables with Timestamp NTZ" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpDir) => {

      spark.sql(
        "CREATE TABLE tbl(c1 STRING, c2 TIMESTAMP, c3 TIMESTAMP_NTZ) " +
          "USING qbeast OPTIONS ('columnsToIndex'='c1')")

      spark.sql("""INSERT INTO tbl (c1, c2, c3)
          | VALUES('foo','2022-01-02 03:04:05.123456','2022-01-02 03:04:05.123456')""".stripMargin)

      val protocol = DeltaLog.forTable(spark, "tbl").unsafeVolatileSnapshot.protocol
      assert(
        protocol ==
          TimestampNTZTableFeature.minProtocolVersion.withFeature(TimestampNTZTableFeature))

      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS")
      val time = formatter.parse("2022-01-02 03:04:05.123456").getTime
      assert(
        spark.table("tbl").head == Row(
          "foo",
          new Timestamp(time),
          LocalDateTime.of(2022, 1, 2, 3, 4, 5, 123456000)))

    })

}
