package io.qbeast.spark.index

import io.qbeast.TestClasses.EcommerceRecord
import io.qbeast.core.model.{CubeId, CubeStatus}
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.PrivateMethodTester

import scala.util.Random

class AppendCompressionTest extends QbeastIntegrationTestSpec with PrivateMethodTester {

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

  "OTree compression" should "index correctly" in withSparkAndTmpDir((spark, tmpDir) => {
    val data = loadTestData(spark)
    data.write
      .mode("overwrite")
      .format("qbeast")
      .option("cubeSize", 5000)
      .option("columnsToIndex", columnsToIndex.mkString(","))
      .save(tmpDir)
    // scalastyle:off println
    val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
    println(metrics)
    metrics.cubeStatuses
      .mapValues(status => {
        val files = status.files.sortBy(_.modificationTime)
        (files.map(_.elementCount), files.map(_.maxWeight))
      })
      .foreach(println)
  })

  "OTree compression" should "produce fewer cubes" in withSparkAndTmpDir((spark, tmpDir) => {
    // Write data using SparkOTreeManager,
    // with compression(isReplication = true) and without(isReplication = false)
    // Create index status through QbeastSnapshot for each and compare cube count
  })
  it should "preserve branch continuity" in withSparkAndTmpDir((spark, tmpDir) => {
    // The branch continuity can be corrupted when two intermediate ancestor nodes
    // are compressed but the cube itself is not. By the wey how the read protocol
    // it set up, we would lose data
  })
  it should "preserve sampling accuracy" in withSparkAndTmpDir((spark, tmpDir) => {
    // Write data using compression and test sampling accuracy
  })
  "Append with compression" should "not lose data" in withSparkAndTmpDir((spark, tmpDir) => {
    // Write data using compression
    // Append data using compression and check record count
  })
  it should "produce fewer cubes" in withSparkAndTmpDir((spark, tmpDir) => {
    // Append with and without compression, the compressed tree should have
    // fewer cubes. We can check this by analyzing index metrics
  })
  it should "not compress tree during OPTIMIZATION" in withSparkAndTmpDir((spark, tmpDir) => {
    // Setting isReplication to true should output an empty compressionMap
  })
  it should "not compress ANNOUNCED nor REPLICATED cubes" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      // Analyze the compressionMap to see that ANNOUNCED and REPLICATED cubes
      // all map to themselves
    })
  it should "preserve monotonically increasing branch maxWeights" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      // Write with compression and append with compression
      // Make sure branch maxWeights are monotonically increasing
    })
  it should "preserve sampling accuracy" in withSparkAndTmpDir((spark, tmpDir) => {
    // Write with compression and appending with compression and test sampling accuracy
  })

//  "Appending with tree compression" should "reduce cube count (via cubeMap comparison)" in
//    withSparkAndTmpDir((spark, tmpDir) => {
//      val original = loadTestData(spark)
//      writeTestData(original, columnsToIndex, 5000, tmpDir)
//
//      val deltaLog = DeltaLog.forTable(spark, tmpDir)
//      val snapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
//      val indexStatus = snapshot.loadLatestIndexStatus
//
//      val dataToAppend = createEcommerceInstances(500)
//      val (indexedData, tableChanges) =
//        SparkOTreeManager.index(dataToAppend.toDF(), indexStatus)
//      val cubeSizes = OTreeCompression.computeCubeSizes(indexedData, dimensionCount)
//      val compressionCubeMap = OTreeCompression.accumulativeRollUp(
//        cubeSizes,
//        tableChanges.updatedRevision.desiredCubeSize)
//
//      compressionCubeMap.size shouldBe cubeSizes.size
//      compressionCubeMap.values.toSet.size shouldBe <(cubeSizes.size)
//
//    })

//  it should "reduce cube count (by comparing cube counts from DF to write)" in
//    withExtendedSparkAndTmpDir(
//      new SparkConf().set("spark.qbeast.index.maxAppendCompressionSize", "100000")) {
//      (spark, tmpDir) =>
//        {
//          val original = loadTestData(spark)
//          writeTestData(original, columnsToIndex, 5000, tmpDir)
//
//          val deltaLog = DeltaLog.forTable(spark, tmpDir)
//          val snapshot = DeltaQbeastSnapshot(deltaLog.snapshot)
//          val indexStatus = snapshot.loadLatestIndexStatus
//
//          val appendSize = 500
//          val dataToAppend = createEcommerceInstances(appendSize)
//          val (indexedData, tableChanges) =
//            SparkOTreeManager.index(dataToAppend.toDF(), indexStatus)
//          val dataToWrite =
//            SparkDeltaDataWriter invokePrivate privateCompression(indexedData, tableChanges)
//
//          val cubeCountWithoutCompression = indexedData.select(cubeColumnName).distinct.count
//          val cubeCountWithCompression = dataToWrite.select(cubeColumnName).distinct.count
//
//          cubeCountWithCompression shouldBe <(cubeCountWithoutCompression)
//        }
//    }

  "Appending data with compression" should "not lose data" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val original = loadTestData(spark)
      writeTestData(original, columnsToIndex, 5000, tmpDir)
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      qbeastTable
        .getIndexMetrics()
        .cubeStatuses
        .mapValues(status => {
          val files = status.files.sortBy(_.modificationTime)
          (files.map(_.elementCount), files.map(_.maxWeight))
        })
        .foreach(println)

      var dataSize = original.count
      val appendSize = 50000
      1 to 5 foreach { _ =>
        val dataToAppend = createEcommerceInstances(appendSize)
        dataToAppend.write.mode("append").format("qbeast").save(tmpDir)
        dataSize += appendSize

        val allData = spark.read.format("qbeast").load(tmpDir)
        allData.count shouldBe dataSize

        println()
        qbeastTable
          .getIndexMetrics()
          .cubeStatuses
          .mapValues(status => {
            val files = status.files.sortBy(_.modificationTime)
            (files.map(_.elementCount), files.map(_.maxWeight))
          })
          .foreach(println)
      }

      val smallFilesCount = qbeastTable
        .getIndexMetrics()
        .cubeStatuses
        .values
        .flatMap(status => status.files.map(_.elementCount))
        .count(s => s < 5000 * 0.8)

      println(smallFilesCount)
    })
//
//  it should "maintain branch maxWeights to be monotonically increasing" in
//    withExtendedSparkAndTmpDir(
//      new SparkConf().set("spark.qbeast.index.maxAppendCompressionSize", "100000")) {
//      (spark, tmpDir) =>
//        {
//          val original = loadTestData(spark)
//          writeTestData(original, columnsToIndex, 5000, tmpDir)
//          hasCorrectBranchMaxWeights(spark, tmpDir) shouldBe true
//
//          1 to 10 foreach { _ =>
//            val dataToAppend = createEcommerceInstances(500)
//            dataToAppend.write.mode("append").format("qbeast").save(tmpDir)
//            hasCorrectBranchMaxWeights(spark, tmpDir) shouldBe true
//          }
//        }
//    }
//
//  it should "not corrupt sampling accuracy" in
//    withExtendedSparkAndTmpDir(
//      new SparkConf().set("spark.qbeast.index.maxAppendCompressionSize", "100000")) {
//      (spark, tmpDir) =>
//        {
//          val df = loadTestData(spark)
//          writeTestData(df, columnsToIndex, 5000, tmpDir)
//
//          var dataSize = 99986
//          val appendSize = 500
//          val tolerance = 0.02
//          1 to 10 foreach { _ =>
//            val dataToAppend = createEcommerceInstances(appendSize)
//            dataToAppend.write.mode("append").format("qbeast").save(tmpDir)
//
//            val allData = spark.read.format("qbeast").load(tmpDir)
//            dataSize += appendSize
//
//            Seq(0.1, 0.2, 0.5, 0.7, 0.9).foreach(f => {
//              val sampleSize = allData.sample(f).count.toDouble
//              val margin = dataSize * f * tolerance
//              sampleSize shouldBe (dataSize * f) +- margin
//            })
//          }
//        }
//    }
}
