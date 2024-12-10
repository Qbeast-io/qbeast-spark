package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec

class QbeastInputSourcesTest extends QbeastIntegrationTestSpec {

  "Qbeast" should "throw an error when indexing non-deterministic query columns" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val df = spark.range(100).withColumn("rand", org.apache.spark.sql.functions.rand())
      val e = intercept[AssertionError] {
        df.write.format("qbeast").option("columnsToIndex", "rand").save(tmpDir)
      }
      assert(
        e.getMessage.contains("Column rand is not deterministic.") || e.getMessage.contains(
          "assertion failed: The query is not deterministic."))
  }

  it should "throw an error when indexing non-deterministic LIMIT" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      // Index non-deterministic columns with LIMIT
      val df = spark.range(100).toDF("id").limit(10)
      val e = intercept[AssertionError] {
        df.write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)
      }
      assert(
        e.getMessage.contains("Column rand is not deterministic.") || e.getMessage.contains(
          "assertion failed: The query is not deterministic."))
  }

}
