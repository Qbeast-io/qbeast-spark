package io.qbeast.spark.index

import io.qbeast.TestClasses._
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformerIndexingTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  "Qbeast spark" should "Index tables with string" in withSparkAndTmpDir((spark, tmpDir) => {

    import spark.implicits._
    val source = 0
      .to(10000)
      .map(i => T1(i, s"$i", i.toDouble))
      .toDF()
      .as[T1]

    val cubeSize = 1000

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,b:hashing,c")
      .option("cubeSize", cubeSize)
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

  "Qbeast spark" should "Index tables without string" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = 0
      .to(10000)
      .map(i => T2(i, i.toDouble))
      .toDF()
      .as[T2]

    val cubeSize = 1000

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", cubeSize)
      .save(tmpDir)

    val indexed = spark.read
      .format("qbeast")
      .load(tmpDir)
      .as[T2]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

}
