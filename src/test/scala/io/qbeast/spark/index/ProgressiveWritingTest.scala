package io.qbeast.spark.index

import io.qbeast.TestClasses.EcommerceRecord
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.delta.writer.SparkDeltaDataWriter
import io.qbeast.spark.delta.writer.OTreeRollUpUtils
import io.qbeast.spark.index.QbeastColumns.cubeColumnName
import org.scalatest.PrivateMethodTester

import scala.util.Random

class ProgressiveWritingTest extends QbeastIntegrationTestSpec with PrivateMethodTester {

  // scalastyle:off println
  def createEcommerceInstances(size: Int): Dataset[EcommerceRecord] = {
    val spark = SparkSession.active
    import spark.implicits._

    1.to(size)
      .map(i =>
        EcommerceRecord(
          "2019-10-31 23:59:59 UTC",
          Random.shuffle(Util.eventTypes).head,
          Random.nextInt(60500010 + 1000978) - 1000978,
          Random.nextInt() + 2053013552259662037L,
          Random.shuffle(Util.categoryCode).head,
          Random.shuffle(Util.brand).head,
          Random.nextInt(2574) + Random.nextDouble,
          Random.nextInt(250971670) + 315309190,
          Random.shuffle(Util.userSessions).head))
      .toDF()
      .as[EcommerceRecord]
  }

  val privateCompression: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('treeCompression)

  "Appending with tree compression" should "reduce cube count (via cubeMap comparison)" in
    withExtendedSparkAndTmpDir(
      new SparkConf().set("spark.qbeast.index.maxRollingRecords", "50000")) { (spark, tmpDir) =>
      {
        val original = loadTestData(spark)
        val columnsToIndex = Seq("user_id", "price", "event_type")
        writeTestData(original, columnsToIndex, 5000, tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val snapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
        val indexStatus = snapshot.loadLatestIndexStatus

        val dataToAppend = createEcommerceInstances(5000)
        val (indexedData, tableChanges) =
          SparkOTreeManager.index(dataToAppend.toDF(), indexStatus)
        val cubeSizes = OTreeRollUpUtils.computeCubeSizes(indexedData, columnsToIndex.size)
        val compressionCubeMap = OTreeRollUpUtils.accumulativeRollUp(
          cubeSizes,
          tableChanges.updatedRevision.desiredCubeSize)

        cubeSizes.size shouldBe compressionCubeMap.size
        cubeSizes.size shouldBe >(compressionCubeMap.values.toSet.size)
      }
    }

  it should "reduce cube count (by comparing cube counts from DF to write)" in
    withExtendedSparkAndTmpDir(
      new SparkConf().set("spark.qbeast.index.maxRollingRecords", "100000")) { (spark, tmpDir) =>
      {
        val original = loadTestData(spark)
        val columnsToIndex = Seq("user_id", "price", "event_type")
        writeTestData(original, columnsToIndex, 5000, tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val snapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
        val indexStatus = snapshot.loadLatestIndexStatus

        val appendSize = 500
        val dataToAppend = createEcommerceInstances(appendSize)
        val (indexedData, tableChanges) =
          SparkOTreeManager.index(dataToAppend.toDF(), indexStatus)
        val dataToWrite =
          SparkDeltaDataWriter invokePrivate privateCompression(indexedData, tableChanges)

        val cubeCountWithoutCompression = indexedData.select(cubeColumnName).distinct.count
        val cubeCountWithCompression = dataToWrite.select(cubeColumnName).distinct.count

        cubeCountWithoutCompression shouldBe >(cubeCountWithCompression)
      }
    }

  it should "not lose data" in withExtendedSparkAndTmpDir(
    new SparkConf().set("spark.qbeast.index.maxRollingRecords", "100000")) { (spark, tmpDir) =>
    {
      val original = loadTestData(spark)
      val columnsToIndex = Seq("user_id", "price", "event_type")
      writeTestData(original, columnsToIndex, 5000, tmpDir)

      var dataSize = original.count
      val appendSize = 500
      1 to 10 foreach { _ =>
        val dataToAppend = createEcommerceInstances(appendSize)
        dataToAppend.write.mode("append").format("qbeast").save(tmpDir)
        dataSize += appendSize

        val allData = spark.read.format("qbeast").load(tmpDir)

        dataSize shouldBe allData.count
      }
    }
  }

}

object Util {

  val brand: Seq[String] = Seq(
    "yokohama",
    "apple",
    "samsung",
    "sonel",
    "sigma",
    "ariston",
    "greenland",
    "kettler",
    "cartier",
    "rieker",
    "bioderma",
    "tuffoni",
    "welss",
    "tega")

  val categoryCode: Seq[String] =
    Seq(
      "computers.ebooks",
      "apparel.shoes.slipons",
      "computers.peripherals.keyboard",
      "electronics.video.projector",
      "appliances.kitchen.coffee_grinder",
      "sport.snowboard",
      "electronics.camera.video",
      "apparel.shirt",
      "electronics.audio.headphone",
      "auto.accessories.radar")

  val eventTypes: Seq[String] = Seq("purchase", "view", "cart")

  val userSessions: Seq[String] =
    Seq(
      "efeb908a-f2c1-4ddd-8f49-361c94a0967b",
      "7860ab49-f0ee-403a-b059-02d47489cc3c",
      "f859c16b-0a95-4afb-a01e-08e9735083de",
      "4fedbad9-8d05-4e89-9d34-defd5d9e0384",
      "98062ef5-7bc7-4f93-bd35-972f28a3e043",
      "777e076a-8fd8-49aa-ba20-a9d811ff5f7f",
      "3be059a9-88b1-45c4-b54b-131f6d9ab5ea",
      "4c0f37cd-24b5-447f-9017-caa3cc845886",
      "4fcd0d55-76d3-4950-8f4f-0e43623f52d1")

}
