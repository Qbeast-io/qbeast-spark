package io.qbeast.spark.index

import io.qbeast.QbeastIntegrationTestSpec

class SparkPlanAnalyzerTest extends QbeastIntegrationTestSpec {

  object SparkPlanAnalyzerTesting extends SparkPlanAnalyzer

  private lazy val nonDeterministicUDF = org.apache.spark.sql.functions
    .udf(() => {
      scala.util.Random.nextInt()
    })
    .asNondeterministic()

  "SparkPlanAnalyzer" should "detect underterminism in LIMIT" in withSparkAndTmpDir {
    (spark, _) =>
      val df = spark
        .range(10)
        .toDF("id")
        .limit(5)
      val columnsToAnalyze = Seq("id")
      val isDeterministic =
        SparkPlanAnalyzerTesting.analyzeDataFrameDeterminism(df, columnsToAnalyze)
      isDeterministic shouldBe false
  }

  it should "detect undeterminism in SAMPLE" in withSparkAndTmpDir { (spark, _) =>
    val df = spark
      .range(10)
      .toDF("id")
      .sample(0.1)
    val columnsToAnalyze = Seq("id")
    val isDeterministic =
      SparkPlanAnalyzerTesting.analyzeDataFrameDeterminism(df, columnsToAnalyze)
    isDeterministic shouldBe false
  }

  it should "detect non-determinism in non-deterministic columns" in withSparkAndTmpDir {
    (spark, _) =>
      val df = spark
        .range(10)
        .withColumn("non_deterministic_col", nonDeterministicUDF())

      val columnsToAnalyze = Seq("non_deterministic_col")
      val isDeterministic =
        SparkPlanAnalyzerTesting.analyzeDataFrameDeterminism(df, columnsToAnalyze)
      isDeterministic shouldBe false
  }

  it should "detect non-deterministic query filters" in withSparkAndTmpDir { (spark, tmpDir) =>
    val df = spark
      .range(10)
      .toDF("id")
      .filter(
        nonDeterministicUDF() > 5
      ) // The filter contains non-deterministic predicates that can affect the results when executed multiple times

    val columnsToAnalyze = Seq("id")
    val isDeterministic =
      SparkPlanAnalyzerTesting.analyzeDataFrameDeterminism(df, columnsToAnalyze)
    isDeterministic shouldBe false

  }

  it should "return true if no columnsToAnalyze are provided" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val df = spark.range(10).toDF("id")
      val isDeterministic = SparkPlanAnalyzerTesting.analyzeDataFrameDeterminism(df, Seq.empty)
      isDeterministic shouldBe true
  }

  it should "return true if all columns are deterministic" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val df = spark.range(10).toDF("id")
      val columnsToAnalyze = Seq("id")
      val isDeterministic =
        SparkPlanAnalyzerTesting.analyzeDataFrameDeterminism(df, columnsToAnalyze)
      isDeterministic shouldBe true
  }

  it should "return true if filters are deterministic" in withSparkAndTmpDir { (spark, tmpDir) =>
    val df = spark.range(10).toDF("id").filter("id > 5")
    val isDeterministic = SparkPlanAnalyzerTesting.analyzeDataFrameDeterminism(df, Seq("id"))
    isDeterministic shouldBe true
  }

  it should "mark a Qbeast Sample as deterministic" in withSparkAndTmpDir { (spark, tmpDir) =>
    val qbeastDir = tmpDir + "/qbeast"
    val df = spark.range(10).toDF("id")
    df.write.format("qbeast").option("columnsToIndex", "id").save(qbeastDir)

    val qbeastDF = spark.read.format("qbeast").load(qbeastDir)
    val sampleDF = qbeastDF.sample(0.5)
    val isDeterministic =
      SparkPlanAnalyzerTesting.analyzeDataFrameDeterminism(sampleDF, Seq("id"))
    isDeterministic shouldBe true
  }

}
