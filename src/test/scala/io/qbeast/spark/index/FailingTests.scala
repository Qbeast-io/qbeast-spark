package io.qbeast.spark.index

import io.qbeast.TestClasses.{Client3, T1, T2, TestStrings}
import io.qbeast.core.model.{IndexStatus, QTableID}
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.{Dataset, SparkSession}

class FailingTests extends QbeastIntegrationTestSpec with IndexTestChecks {

  // TODO this was failing on Eric's computer, but now it's working
  "In IndexTest the algorithm" should "work with smaller values" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {

        val rdd =
          spark.sparkContext.parallelize(
            0.to(1000)
              .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))

        val df = spark.createDataFrame(rdd)
        val smallCubeSize = 10
        val rev = SparkRevisionFactory.createNewRevision(
          QTableID("test"),
          df.schema,
          Map("columnsToIndex" -> "age,val2", "cubeSize" -> smallCubeSize.toString))

        val (indexed, tc) = oTreeAlgorithm.index(df, IndexStatus(rev))
        val weightMap = tc.cubeWeights

        checkDFSize(indexed, df)
        checkCubes(weightMap)
        checkWeightsIncrement(weightMap)
        checkCubesOnData(weightMap, indexed, 2)
        checkCubeSize(tc, rev, indexed)
      }
    }
  }

  // Write source data indexing all columns and read it back
  private def writeAndReadDF(source: Dataset[_], tmpDir: String, spark: SparkSession) = {
    source.write
      .format("qbeast")
      .option("columnsToIndex", source.columns.mkString(","))
      .option("cubeSize", 10)
      .save(tmpDir)

    spark.read
      .format("qbeast")
      .load(tmpDir)
  }

  // TODO seems tests are failing when trying to index string/hashed columns
  "In TransformerIndexingTest" should "Index tables with string" in withSparkAndTmpDir(
    (spark, tmpDir) => {

      import spark.implicits._
      val source = 0
        .to(1000)
        .map(i => T1(i, s"$i", i.toDouble))
        .toDF()
        .as[T1]

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a,b,c")
        .option("cubeSize", 10)
        .save(tmpDir)

      val indexed = spark.read
        .format("qbeast")
        .load(tmpDir)
        .as[T1]

      source.count() shouldBe indexed.count()

      assertSmallDatasetEquality[T1](
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

    })

  it should
    "index tables with hashing configuration" in withSparkAndTmpDir((spark, tmpDir) => {
      import spark.implicits._
      val source = 0
        .to(1000)
        .map(i => T2(i, i.toDouble))
        .toDF()
        .as[T2]

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a:hashing,c:hashing")
        .option("cubeSize", 10)
        .save(tmpDir)

      val indexed = spark.read
        .format("qbeast")
        .load(tmpDir)
        .as[T2]

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

    })

  it should "index tables with all String" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = 0
      .to(1000)
      .map(i => TestStrings(s"${i * 2}", s"$i", s"$i$i"))
      .toDF()
      .as[TestStrings]
    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestStrings]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

}
