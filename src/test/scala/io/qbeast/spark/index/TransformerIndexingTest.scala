package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class T1(a: Int, b: String, c: Double)
case class T2(a: Int, c: Double)

class TransformerIndexingTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  "Qbeast spark" should "Index tables with string" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = 0
      .to(1000)
      .map(i => T1(i, s"$i", i.toDouble))
      .toDF()
      .as[T1]

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,b:hashing,c")
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
    val f = indexed
      .withColumn("file_name", input_file_name())
      .groupBy(col("file_name"))
      .count()

    f.filter(col("count") > 15).isEmpty shouldBe true

    f.select(avg(col("count"))) shouldBe 10 +- 2
  })

  "Qbeast spark" should "Index tables without string" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = 0
      .to(1000)
      .map(i => T2(i, i.toDouble))
      .toDF()
      .as[T2]

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", 10)
      .save(tmpDir)

    val indexed = spark.read
      .format("qbeast")
      .load(tmpDir)
      .as[T2]

    val p = spark.read
      .parquet(tmpDir)
      .as[T2]

    p.count() shouldBe source.count()

    indexed.count() shouldBe source.count()
    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)
    val f = indexed
      .withColumn("file_name", input_file_name())
      .groupBy(col("file_name"))
      .count()

    f.filter(col("count") > 15).isEmpty shouldBe true

    f.select(avg(col("count"))) shouldBe 10 +- 2
  })

  "Qbeast spark" should "Index tables without indexed column with string type" in
    withSparkAndTmpDir((spark, tmpDir) => {
      import spark.implicits._
      val source = 0
        .to(1000)
        .map(i => T1(i, s"$i", i.toDouble))
        .toDF()
        .as[T1]

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", "10")
        .save(tmpDir)

      val indexed = spark.read
        .format("qbeast")
        .load(tmpDir)
        .as[T1]

      val p = spark.read
        .parquet(tmpDir)
        .as[T1]

      val f = indexed
        .withColumn("file_name", input_file_name())
        .groupBy(col("file_name"))
        .count()

      p.count() shouldBe source.count()

      indexed.count() shouldBe source.count()
      assertSmallDatasetEquality(
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

      f.filter(col("count") > 15).isEmpty shouldBe true

      f.select(avg(col("count"))) shouldBe 10 +- 2
    })

}
