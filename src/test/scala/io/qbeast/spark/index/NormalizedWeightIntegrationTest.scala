package io.qbeast.spark.index

import io.qbeast.TestClasses.Client3
import io.qbeast.spark.QbeastIntegrationTestSpec
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
        files.map(_.tags(TagUtils.maxWeight).toInt).collect()(0) shouldBe Int.MaxValue
        files.map(_.tags(TagUtils.state)).collect()(0) shouldBe "FLOODED"

    }
  "CubeNormalizedWeights" should
    "write a the right Weight with a full cube split in 3 blocks" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      val cubeSize = 10000
      val df = createDF(cubeSize).repartition(3)
      val names = List("age", "val2")
      // val dimensionCount = names.length
      df.write
        .format("qbeast")
        .mode("overwrite")
        .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
        .save(tmpDir)

      spark.read.format("qbeast").load(tmpDir).count() shouldBe cubeSize

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val files = deltaLog.snapshot.allFiles.collect()
      files.length shouldBe 1

      val tags = files.map(_.tags)
      val root = tags.filter(_(TagUtils.cube) == "").head

      root(TagUtils.maxWeight).toInt shouldBe Int.MaxValue
      root(TagUtils.state) shouldBe "FLOODED"
      root(TagUtils.elementCount).toInt shouldBe cubeSize

    }
}
