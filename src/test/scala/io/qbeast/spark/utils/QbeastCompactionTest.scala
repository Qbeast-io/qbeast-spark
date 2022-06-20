package io.qbeast.spark.utils

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.functions.{col, input_file_name}

class QbeastCompactionTest extends QbeastIntegrationTestSpec {

  "Compaction command" should
    "reduce the number of files" in withSparkAndTmpDir { (spark, tmpDir) =>
      {

        val data = loadTestData(spark)
        writeTestData(data, Seq("user_id", "product_id"), 10000, tmpDir)
        writeTestData(
          data.withColumn("product_id", col("product_id") * 2),
          Seq("user_id", "product_id"),
          10000,
          tmpDir,
          "append")

        val indexed = spark.read.format("qbeast").load(tmpDir)
        val originalNumOfFiles = indexed.select(input_file_name()).distinct().count()

        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.compact()

        indexed.select(input_file_name()).distinct().count() shouldBe <(originalNumOfFiles)

      }
    }

}
