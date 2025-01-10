package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec

class QbeastInputSourcesTest extends QbeastIntegrationTestSpec {

  private val nonDeterministicColumns =
    Seq("rand()", "uuid()")

  "Qbeast" should "throw an error when indexing non-deterministic query columns" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      nonDeterministicColumns.foreach(column => {
        val location = tmpDir + "/" + column
        val df = spark
          .range(10)
          .withColumn("non_deterministic_col", org.apache.spark.sql.functions.expr(column))
        val e = intercept[AssertionError] {
          df.write
            .format("qbeast")
            .option("columnsToIndex", "non_deterministic_col")
            .save(location)
        }
        assert(e.getMessage.contains("assertion failed: The source query is non-deterministic."))
      })
  }

  it should "allow to write non-deterministic columns when they are not being indexed" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      nonDeterministicColumns.foreach(column => {
        val location = tmpDir + "/" + column
        val df = spark
          .range(10)
          .toDF("id")
          .withColumn("non_deterministic_col", org.apache.spark.sql.functions.expr(column))
        df.write.format("qbeast").option("columnsToIndex", "id").save(location)
        assertSmallDatasetEquality(
          df,
          spark.read.format("qbeast").load(location),
          ignoreNullable = true)
      })
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

  it should "throw an error with non-deterministic filter query" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val df = spark.range(10).toDF("id")
      val e = intercept[AssertionError] {
        df.filter(
          "rand() > 0.5"
        ) // The filter contains non-deterministic predicates that can affect the results when executed multiple times
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

  it should "allow indexing Qbeast Samples" in withSparkAndTmpDir { (spark, tmpDir) =>
    val qbeastDir = tmpDir + "/qbeast"
    val finalDir = tmpDir + "/final"
    val df = spark.range(10).toDF("id")
    df.write.format("qbeast").option("columnsToIndex", "id").save(qbeastDir)

    val qbeastDF = spark.read.format("qbeast").load(qbeastDir)
    val sampleDF = qbeastDF.sample(0.5)
    sampleDF.write.format("qbeast").option("columnsToIndex", "id").save(finalDir)

    val finalDF = spark.read.format("qbeast").load(finalDir)
    assertSmallDatasetEquality(sampleDF, finalDF)
  }

}
