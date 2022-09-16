package io.qbeast.spark.index

import io.qbeast.TestClasses.EcommerceRecord
import io.qbeast.core.model.{CubeId, CubeStatus}
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
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

  val columnsToIndex: Seq[String] = Seq("user_id", "price", "event_type")
  val dimensionCount: Int = columnsToIndex.size

  val (user_id_min, user_id_max) = (315309190, 566280860)
  val price_max = 2574
  val eventTypes: Seq[String] = Seq("purchase", "view", "cart")

  def createEcommerceInstances(size: Int): Dataset[EcommerceRecord] = {
    val spark = SparkSession.active
    import spark.implicits._

    1.to(size)
      .map(i =>
        EcommerceRecord(
          event_time = "2019-10-31 18:50:55 UTC",
          event_type = Random.shuffle(eventTypes).head,
          product_id = Random.nextInt(60500010 - 1000978) + 1000978,
          category_id = 2053013552259662037L,
          category_code = "accessories.bag",
          brand = "a-case",
          price = Random.nextInt(price_max) + Random.nextDouble,
          user_id = Random.nextInt(user_id_max - user_id_min) + user_id_min,
          user_session = "00029da2-57bf-4f8d-bd76-2631220754f1"))
      .toDF()
      .as[EcommerceRecord]
  }

  val privateCompression: PrivateMethod[DataFrame] = PrivateMethod[DataFrame]('treeCompression)

  def branchMaxWeightCheck(
      cube: CubeId,
      parentWeight: NormalizedWeight,
      cubeWeights: Map[CubeId, CubeStatus]): Boolean = {
    if (cubeWeights.contains(cube)) {
      val cubeMaxWeight = cubeWeights(cube).normalizedWeight

      if (cubeMaxWeight <= parentWeight) {
        false
      } else {
        cube.children
          .forall(c => branchMaxWeightCheck(c, cubeMaxWeight, cubeWeights))
      }
    } else {
      val nextLevel = cube.children
        .filter(cubeWeights.contains)

      nextLevel.isEmpty || nextLevel.forall(c =>
        branchMaxWeightCheck(c, parentWeight, cubeWeights))
    }
  }

  def hasCorrectBranchMaxWeights(spark: SparkSession, path: String): Boolean = {
    val qbeastTable = QbeastTable.forPath(spark, path)
    val index = qbeastTable.getIndexMetrics().cubeStatuses
    val root = CubeId.root(qbeastTable.indexedColumns().size)
    branchMaxWeightCheck(root, -1, index)
  }

  "Appending with tree compression" should "reduce cube count (via cubeMap comparison)" in
    withExtendedSparkAndTmpDir(
      new SparkConf().set("spark.qbeast.index.maxRollingRecords", "100000")) { (spark, tmpDir) =>
      {
        val original = loadTestData(spark)
        writeTestData(original, columnsToIndex, 5000, tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val snapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
        val indexStatus = snapshot.loadLatestIndexStatus

        val dataToAppend = createEcommerceInstances(500)
        val (indexedData, tableChanges) =
          SparkOTreeManager.index(dataToAppend.toDF(), indexStatus)
        val cubeSizes = OTreeRollUpUtils.computeCubeSizes(indexedData, dimensionCount)
        val compressionCubeMap = OTreeRollUpUtils.accumulativeRollUp(
          cubeSizes,
          tableChanges.updatedRevision.desiredCubeSize)

        compressionCubeMap.size shouldBe cubeSizes.size
        compressionCubeMap.values.toSet.size shouldBe <(cubeSizes.size)
      }
    }

  it should "reduce cube count (by comparing cube counts from DF to write)" in
    withExtendedSparkAndTmpDir(
      new SparkConf().set("spark.qbeast.index.maxRollingRecords", "100000")) { (spark, tmpDir) =>
      {
        val original = loadTestData(spark)
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

        cubeCountWithCompression shouldBe <(cubeCountWithoutCompression)
      }
    }

  it should "not lose data" in withExtendedSparkAndTmpDir(
    new SparkConf().set("spark.qbeast.index.maxRollingRecords", "100000")) { (spark, tmpDir) =>
    {
      val original = loadTestData(spark)
      writeTestData(original, columnsToIndex, 5000, tmpDir)

      var dataSize = original.count
      val appendSize = 500
      1 to 10 foreach { _ =>
        val dataToAppend = createEcommerceInstances(appendSize)
        dataToAppend.write.mode("append").format("qbeast").save(tmpDir)
        dataSize += appendSize

        val allData = spark.read.format("qbeast").load(tmpDir)

        allData.count shouldBe dataSize
      }
    }
  }

  it should "maintain branch maxWeights to be monotonically increasing" in
    withExtendedSparkAndTmpDir(
      new SparkConf().set("spark.qbeast.index.maxRollingRecords", "100000")) { (spark, tmpDir) =>
      {
        val original = loadTestData(spark)
        writeTestData(original, columnsToIndex, 5000, tmpDir)
        hasCorrectBranchMaxWeights(spark, tmpDir) shouldBe true

        1 to 10 foreach { _ =>
          val dataToAppend = createEcommerceInstances(500)
          dataToAppend.write.mode("append").format("qbeast").save(tmpDir)
          hasCorrectBranchMaxWeights(spark, tmpDir) shouldBe true
        }
      }
    }

  it should "not corrupt inline sampling" in
    withExtendedSparkAndTmpDir(
      new SparkConf().set("spark.qbeast.index.maxRollingRecords", "100000")) { (spark, tmpDir) =>
      {
        val df = loadTestData(spark)
        writeTestData(df, columnsToIndex, 5000, tmpDir)

        var dataSize = 99986
        val appendSize = 500
        val tolerance = 0.02
        1 to 10 foreach { _ =>
          val dataToAppend = createEcommerceInstances(appendSize)
          dataToAppend.write.mode("append").format("qbeast").save(tmpDir)

          val allData = spark.read.format("qbeast").load(tmpDir)
          dataSize += appendSize

          Seq(0.1, 0.2, 0.5, 0.7, 0.9).foreach(f => {
            val sampleSize = allData.sample(f).count.toDouble
            val margin = dataSize * f * tolerance
            sampleSize shouldBe (dataSize * f) +- margin
          })
        }
      }
    }
}
