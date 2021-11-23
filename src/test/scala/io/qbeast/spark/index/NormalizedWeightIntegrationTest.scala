package io.qbeast.spark.index

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.OTreeAlgorithmTest.Client3
import io.qbeast.spark.utils.TagUtils
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, SparkSession}

class NormalizedWeightIntegrationTest extends QbeastIntegrationTestSpec {

  def createDF(size: Int): DataFrame = {
    val spark = SparkSession.active

    val rdd =
      spark.sparkContext.parallelize(
        1.to(size)
          .map(i => Client3(i * i, s"student-$i", i, i * 1000 + 123, i * 2567.3432143)))
    assert(rdd.count() == size)
    spark.createDataFrame(rdd)
  }

  "CubeNormalizedWeights" should
    "write a the right Weight with a single full file" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        import spark.implicits._
        val cubeSize = 10000
        val df = createDF(cubeSize).repartition(1)
        val names = List("age", "val2")
        // val dimensionCount = names.length
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        spark.read.format("qbeast").load(tmpDir).count() shouldBe cubeSize

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val files = deltaLog.snapshot.allFiles
        files.count() shouldBe 1
        files.map(_.tags(TagUtils.maxWeight).toInt).collect()(0) should be < Int.MaxValue
        files.map(_.tags(TagUtils.state)).collect()(0) shouldBe "FLOODED"

    }
  "CubeNormalizedWeights" should
    "write a the right Weight with a full cube split in 3 blocks" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._
      val cubeSize = 10000
      val df = createDF(cubeSize).repartition(3)
      val names = List("age", "val2")
      // val dimensionCount = names.length
      df.write
        .format("qbeast")
        .mode("overwrite")
        .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> "10"))
        .save(tmpDir)

      spark.read.format("qbeast").load(tmpDir).count() shouldBe cubeSize

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val files = deltaLog.snapshot.allFiles

      val tags = files.map(_.tags).collect()
      val root = tags.filter(_(TagUtils.cube) == "").head
      val child = tags.filter(_(TagUtils.cube) == "A").head
      root(TagUtils.maxWeight).toInt should be < Int.MaxValue
      root(TagUtils.state) shouldBe "FLOODED"

      child(TagUtils.maxWeight).toInt shouldBe Int.MaxValue
      child(TagUtils.state) shouldBe "FLOODED" // EVEN though it is not...

      child(TagUtils.elementCount).toInt + root(TagUtils.elementCount).toInt shouldBe cubeSize

    }
}
