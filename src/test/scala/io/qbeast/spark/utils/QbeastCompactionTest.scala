package io.qbeast.spark.utils

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

class QbeastCompactionTest extends QbeastIntegrationTestSpec {

  "Compaction command" should
    "reduce the number of files" in withExtendedSparkAndTmpDir(
      new SparkConf().set("spark.qbeast.compact.minFileSize", "1")) { (spark, tmpDir) =>
      {

        val data = loadTestData(spark)

        // Creating four batches of 20000 elements each one
        // So they all go to the root cube
        // and we can compact them later
        val batch = data.limit(20000)

        // First write
        writeTestData(batch, Seq("user_id", "product_id"), 100000, tmpDir)

        // Append more data
        writeTestData(batch, Seq("user_id", "product_id"), 100000, tmpDir, "append")

        writeTestData(batch, Seq("user_id", "product_id"), 100000, tmpDir, "append")

        writeTestData(batch, Seq("user_id", "product_id"), 100000, tmpDir, "append")

        val indexed = spark.read.format("qbeast").load(tmpDir)
        val originalNumOfFiles = indexed.select(input_file_name()).distinct().count()

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.compact()

        indexed.select(input_file_name()).distinct().count() shouldBe <(originalNumOfFiles)

      }
    }

}
