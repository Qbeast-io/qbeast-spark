package io.qbeast.spark.utils

import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.PrivateMethodTester

class ConvertToQbeastTest extends QbeastIntegrationTestSpec with PrivateMethodTester {
  val columnsToIndex: Seq[String] = Seq("user_id", "price", "event_type")
  val dataSize = 99986 // loadTestData(spark).count

  def convertFormatsFromTo(
      sourceFormat: String,
      readFormat: String,
      spark: SparkSession,
      dir: String,
      columnsToIndex: Seq[String] = columnsToIndex): DataFrame = {
    val data = loadTestData(spark)
    data.write.mode("overwrite").format(sourceFormat).save(dir)

    ConvertToQbeastCommand(dir, sourceFormat, columnsToIndex).run(spark)

    spark.read.format(readFormat).load(dir)
  }

  val privateIsQbeast: PrivateMethod[Boolean] = PrivateMethod[Boolean]('isQbeastTable)

  "ConvertToQbeastCommand" should "convert a Delta Table into a Qbeast Table" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val qDf = convertFormatsFromTo("delta", "qbeast", spark, tmpDir)
      qDf.count shouldBe dataSize
    })

  it should "convert a Parquet Table into a Qbeast Table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val qDf = convertFormatsFromTo("parquet", "qbeast", spark, tmpDir)
      qDf.count shouldBe dataSize
    })

  it should "throw an error when attempting to convert an unsupported format" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = loadTestData(spark)
      df.write.mode("overwrite").json(tmpDir)

      an[UnsupportedOperationException] shouldBe thrownBy(
        ConvertToQbeastCommand(tmpDir, "json", columnsToIndex).run(spark))
    })

//  it should "throw an error when the file format does not match" in withSparkAndTmpDir(
//    (spark, tmpDir) => {
//      val df = loadTestData(spark)
//      // write as json
//      df.write.mode("overwrite").json(tmpDir)
//
//      // Run the command
//      // read as delta
//      an[AnalysisException] shouldBe thrownBy(
//        ConvertToQbeastCommand(tmpDir, "delta", columnsToIndex)
//          .run(spark))
//
//    })

  it should "throw an error if columnsToIndex are not found in table schema" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val nonExistentColumns = Seq("a", "b")

      an[RuntimeException] shouldBe thrownBy(
        convertFormatsFromTo("delta", "qbeast", spark, tmpDir, nonExistentColumns))
    })

  "ConvertToQbeastCommand's idempotence" should "not try to convert a converted table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      convertFormatsFromTo("parquet", "qbeast", spark, tmpDir)
      ConvertToQbeastCommand(tmpDir, "parquet", columnsToIndex).run(spark)

      val df = spark.read.format("qbeast").load(tmpDir)
      val deltaLog = DeltaLog.forTable(spark, tmpDir)

      df.count shouldBe dataSize
      // Converting parquet to delta creates snapshot version 0, and its
      // conversion to qbeast creates snapshot version 1. If the second
      // conversion gets executed, it'd produce a snapshot version 2
      deltaLog.snapshot.version shouldBe 1
    })

  it should "not try to convert a qbeast table" in withSparkAndTmpDir((spark, tmpDir) => {
    val data = loadTestData(spark)
    writeTestData(data, columnsToIndex, 50000, tmpDir)

    ConvertToQbeastCommand(tmpDir, "parquet", columnsToIndex) invokePrivate privateIsQbeast(
      spark) shouldBe true
  })

  it should "create correct OTree metrics" in withSparkAndTmpDir((spark, tmpDir) => {
    convertFormatsFromTo("delta", "qbeast", spark, tmpDir)

    val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()

    metrics.elementCount shouldBe dataSize
    metrics.cubeCount shouldBe 1
  })

  it should "allow correct execution of Analyze and Optimize" in withSparkAndTmpDir(
    (spark, tmpDir) => {})

  it should "allow correct execution of Compaction" in withSparkAndTmpDir((spark, tmpDir) => {})

  "A converted delta table" should "be readable using delta" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val qDf = convertFormatsFromTo("delta", "delta", spark, tmpDir)
      qDf.count shouldBe dataSize
    })

  it should "be readable using parquet" in withSparkAndTmpDir((spark, tmpDir) => {
    val qDf = convertFormatsFromTo("delta", "parquet", spark, tmpDir)
    qDf.count shouldBe dataSize
  })

  "A converted parquet table" should "be readable using delta" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val qDf = convertFormatsFromTo("parquet", "delta", spark, tmpDir)
      qDf.count shouldBe dataSize
    })

  it should "be readable using parquet" in withSparkAndTmpDir((spark, tmpDir) => {
    val qDf = convertFormatsFromTo("parquet", "parquet", spark, tmpDir)
    qDf.count shouldBe dataSize
  })
}
