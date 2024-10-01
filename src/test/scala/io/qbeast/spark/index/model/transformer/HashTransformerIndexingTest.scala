package io.qbeast.spark.index.model.transformer

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.T2
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HashTransformerIndexingTest
    extends AnyFlatSpec
    with Matchers
    with QbeastIntegrationTestSpec {

  "Qbeast Spark" should
    "index tables with hashing configuration" in withSparkAndTmpDir((spark, tmpDir) => {
      import spark.implicits._
      val source = 0
        .to(100000)
        .map(i => T2(i, i.toDouble))
        .toDF()
        .as[T2]

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a:hashing,c:hashing")
        .option("cubeSize", 10000)
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

}
