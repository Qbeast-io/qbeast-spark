package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec

class QbeastInputSourcesTest extends QbeastIntegrationTestSpec {

  "Qbeast" should "throw an error when indexing non-deterministic query columns" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val df = spark.range(10).withColumn("rand", org.apache.spark.sql.functions.rand())
      val e = intercept[AssertionError] {
        df.write.format("qbeast").option("columnsToIndex", "rand").save(tmpDir)
      }
      assert(e.getMessage.contains("assertion failed: The source query is non-deterministic."))
  }

  it should "throw an error when indexing random UUID" in withSparkAndTmpDir { (spark, tmpDir) =>
    val df = spark.range(10).withColumn("uuid", org.apache.spark.sql.functions.uuid())
    val e = intercept[AssertionError] {
      df.write.format("qbeast").option("columnsToIndex", "uuid").save(tmpDir)
    }
    assert(e.getMessage.contains("assertion failed: The source query is non-deterministic."))
  }

  it should "throw an error when indexing non-deterministic LIMIT" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      // Index non-deterministic columns with LIMIT
      val df = spark.range(10).toDF("id")
      val e = intercept[AssertionError] {
        df.limit(5).write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)
      }
      assert(e.getMessage.contains("assertion failed: The source query is non-deterministic."))
  }

  it should "throw an error when indexing non-deterministic SAMPLE" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      // Index non-deterministic columns with SAMPLE
      val df = spark.range(10).toDF("id")
      val e = intercept[AssertionError] {
        df.sample(0.5).write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)
      }
      assert(e.getMessage.contains("assertion failed: The source query is non-deterministic."))
  }

  it should "throw an error with undeterministic filter query" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val df = spark.range(10).toDF("id")
      val e = intercept[AssertionError] {
        df.filter("rand() > 0.5")
          .write
          .format("qbeast")
          .option("columnsToIndex", "id")
          .save(tmpDir)
      }
      assert(e.getMessage.contains("assertion failed: The source query is non-deterministic."))
  }

  it should "allow indexing deterministic filters" in withSparkAndTmpDir { (spark, tmpDir) =>
    val df = spark.range(10).toDF("id")
    df.filter("id > 5").write.format("qbeast").option("columnsToIndex", "id").save(tmpDir)
  }

}
