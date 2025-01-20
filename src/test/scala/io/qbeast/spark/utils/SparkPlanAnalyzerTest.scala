package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec

class SparkPlanAnalyzerTest extends QbeastIntegrationTestSpec {

  object SparkPlanAnalyzerTesting extends SparkPlanAnalyzer {

    override def isDataFramePlanDeterministic(df: org.apache.spark.sql.DataFrame): Boolean =
      super.isDataFramePlanDeterministic(df)

  }

  private lazy val nonDeterministicUDF = org.apache.spark.sql.functions
    .udf(() => {
      scala.util.Random.nextInt()
    })
    .asNondeterministic()

  "SparkPlanAnalyzer" should "detect non-determinism in LIMIT" in withSparkAndTmpDir {
    (spark, _) =>
      val df = spark
        .range(10)
        .toDF("id")
        .limit(5)
      val isDeterministic =
        SparkPlanAnalyzerTesting.isDataFramePlanDeterministic(df)
      isDeterministic shouldBe false
  }

  it should "detect non-determinism in SAMPLE" in withSparkAndTmpDir { (spark, _) =>
    val df = spark
      .range(10)
      .toDF("id")
      .sample(0.1)
    val isDeterministic =
      SparkPlanAnalyzerTesting.isDataFramePlanDeterministic(df)
    isDeterministic shouldBe false
  }

  it should "detect non-deterministic query filters" in withSparkAndTmpDir { (spark, tmpDir) =>
    val df = spark
      .range(10)
      .toDF("id")
      .filter(
        nonDeterministicUDF() > 5
      ) // The filter contains non-deterministic predicates that can affect the results when executed multiple times

    val isDeterministic =
      SparkPlanAnalyzerTesting.isDataFramePlanDeterministic(df)
    isDeterministic shouldBe false

  }

  it should "return true if all columns are deterministic" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val df = spark.range(10).toDF("id")
      val isDeterministic =
        SparkPlanAnalyzerTesting.isDataFramePlanDeterministic(df)
      isDeterministic shouldBe true
  }

  it should "return true if filters are deterministic" in withSparkAndTmpDir { (spark, tmpDir) =>
    val df = spark.range(10).toDF("id").filter("id > 5")
    val isDeterministic =
      SparkPlanAnalyzerTesting.isDataFramePlanDeterministic(df)
    isDeterministic shouldBe true
  }

  it should "mark a Qbeast Sample as deterministic" in withSparkAndTmpDir { (spark, tmpDir) =>
    val qbeastDir = tmpDir + "/qbeast"
    val df = spark.range(10).toDF("id")
    df.write.format("qbeast").option("columnsToIndex", "id").save(qbeastDir)

    val qbeastDF = spark.read.format("qbeast").load(qbeastDir)
    val sampleDF = qbeastDF.sample(0.5)
    val isDeterministic =
      SparkPlanAnalyzerTesting.isDataFramePlanDeterministic(sampleDF)
    isDeterministic shouldBe true
  }

  it should "detect when the column value is based on a non-deterministic condition" in withSparkAndTmpDir {
    (spark, _) =>
      val df = spark
        .range(10)
        .toDF("id")
        .withColumn("non_deterministic_col", nonDeterministicUDF())
        .withColumn(
          "conditional_col",
          org.apache.spark.sql.functions
            .when(org.apache.spark.sql.functions.col("non_deterministic_col") > 5, 1)
            .otherwise(0))

      val isDeterministic =
        SparkPlanAnalyzerTesting.isDataFramePlanDeterministic(df)
      isDeterministic shouldBe false
  }

  it should "bypass if the query is deterministic" in withSparkAndTmpDir { (spark, _) =>
    val df = spark.range(10).toDF("id")
    val isDeterministic =
      SparkPlanAnalyzerTesting.isDataFramePlanDeterministic(df)
    isDeterministic shouldBe true
  }

}
