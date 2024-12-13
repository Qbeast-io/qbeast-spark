package io.qbeast.spark.delta

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType

class DeltaQbeastStatsIntegrationTest extends QbeastIntegrationTestSpec {

  "Qbeast" should "preserve column stats format with strings" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._
      val data = spark.range(10).withColumn("name", col("id").cast(StringType))
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      val allFiles = DeltaLog.forTable(spark, tmpDir).update().allFiles
      allFiles
        .map(_.stats)
        .first() shouldBe
        """{"numRecords":10,"minValues":{"id":0,"name":"0"},"maxValues":{"id":9,"name":"9"},"nullCount":{"id":0,"name":0}}""".stripMargin

    })

  it should "preserve column stats format with numeric" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._
      val data = spark
        .range(10)
        .withColumn("id_lon", col("id").cast("long"))
        .withColumn("id_int", col("id").cast("int"))
        .withColumn("id_short", col("id").cast("short"))
        .withColumn("id_byte", col("id").cast("byte"))
        .withColumn("id_double", col("id").cast("double"))

      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      val allFiles = DeltaLog.forTable(spark, tmpDir).update().allFiles
      allFiles
        .map(_.stats)
        .first() shouldBe
        """{"numRecords":10,"minValues":{"id":0,"id_lon":0,"id_int":0,"id_short":0,"id_byte":0,"id_double":0.0},"maxValues":{"id":9,"id_lon":9,"id_int":9,"id_short":9,"id_byte":9,"id_double":9.0},"nullCount":{"id":0,"id_lon":0,"id_int":0,"id_short":0,"id_byte":0,"id_double":0}}""".stripMargin

    })

  it should "preserve columnStats with combined string columns" in withQbeastContextSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._
      val dataNumbers = spark.range(10).withColumn("name", col("id").cast(StringType))
      val dataNames = spark.range(10, 20).withColumn("name", lit("a").cast(StringType))
      val data = dataNumbers.union(dataNames)
      data.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)

      val allFiles = DeltaLog.forTable(spark, tmpDir).update().allFiles
      allFiles
        .map(_.stats)
        .first() shouldBe
        """{"numRecords":20,"minValues":{"id":0,"name":"0"},"maxValues":{"id":19,"name":"a"},"nullCount":{"id":0,"name":0}}""".stripMargin

    })

}
