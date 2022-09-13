package io.qbeast.spark.index

import io.qbeast.TestClasses.EcommerceRecord
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Random

class ProgressiveWritingTest extends QbeastIntegrationTestSpec {

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

  "OTree algorithm" should "progressively append data" in withSparkAndTmpDir((spark, tmpDir) => {
    // scalastyle:off println
    val tmpDir = "/tmp/test/"
//    val df = loadTestData(spark)
//    writeTestData(df, Seq("user_id", "price", "event_type"), 5000, tmpDir)

    val qt = QbeastTable.forPath(spark, tmpDir)
    println(qt.getIndexMetrics())

    1 to 10 foreach { _ =>
      val dataToAppend = createEcommerceInstances(5000)
      dataToAppend.write.mode("append").format("qbeast").save(tmpDir)
      println(qt.getIndexMetrics())
    }
  })

  "Compaction" should "reduce the number of small blocks" in withExtendedSparkAndTmpDir(
    new SparkConf().set("spark.qbeast.compact.minFileSize", "1")) { (spark, tmpDir) =>
    {
      val tmpDir = "/tmp/test/"
      val qt = QbeastTable.forPath(spark, tmpDir)
      println(qt.getIndexMetrics())

      qt.compact()

      println(qt.getIndexMetrics())
    }
  }

  "Modifications" should "display cube sizes and parent cube sizes after folding" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val tmpDir = "/tmp/test/"
      val df = loadTestData(spark)
      writeTestData(df, Seq("user_id", "price", "event_type"), 5000, tmpDir)
    })
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
