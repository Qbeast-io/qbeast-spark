package io.qbeast.spark.utils

import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.scalatest.PrivateMethodTester

class ConvertToQbeastTest extends QbeastIntegrationTestSpec with PrivateMethodTester {
  val columnsToIndex: Seq[String] = Seq("user_id", "price", "event_type")
  val partitionColumns: Option[String] = Some("event_type STRING")
  val dataSize = 50000

  def convertFormatsFromTo(
      spark: SparkSession,
      format: String,
      tablePath: String,
      partitionColumns: Option[String] = None,
      columnsToIndex: Seq[String] = columnsToIndex,
      desiredCubeSize: Int = 50000): DataFrame = {
    val data = loadTestData(spark).limit(dataSize)

    if (partitionColumns.isDefined) {
      val cols = partitionColumns.get.split(", ").map(_.split(" ").head)
      data.write.mode("overwrite").partitionBy(cols: _*).format(format).save(tablePath)
    } else data.write.mode("overwrite").format(format).save(tablePath)

    val tableIdentifier = s"$format.`$tablePath`"
    ConvertToQbeastCommand(tableIdentifier, columnsToIndex, desiredCubeSize, partitionColumns)
      .run(spark)

    spark.read.format("qbeast").load(tablePath)
  }

  "ConvertToQbeastCommand" should "convert a delta table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFormatsFromTo(spark, "delta", tmpDir)

      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
      convertedTable.count shouldBe dataSize
      spark.read.parquet(tmpDir).count shouldBe dataSize

      metrics.elementCount shouldBe 0
      metrics.cubeCount shouldBe 0
    })

  it should "convert a PARTITIONED delta table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFormatsFromTo(spark, "delta", tmpDir, partitionColumns)

      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
      convertedTable.count shouldBe dataSize
      metrics.elementCount shouldBe 0
      metrics.cubeCount shouldBe 0
    })

  it should "convert a parquet table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFormatsFromTo(spark, "parquet", tmpDir)

      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
      convertedTable.count shouldBe dataSize
      metrics.elementCount shouldBe 0
      metrics.cubeCount shouldBe 0
    })

  it should "convert a PARTITIONED parquet table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFormatsFromTo(spark, "parquet", tmpDir, partitionColumns)

      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
      convertedTable.count shouldBe dataSize
      metrics.elementCount shouldBe 0
      metrics.cubeCount shouldBe 0
    })

  it should "not try to convert a qbeast table" in withSparkAndTmpDir((spark, tmpDir) => {
    val data = loadTestData(spark).limit(dataSize)
    data.write
      .format("qbeast")
      .option("columnsToIndex", columnsToIndex.mkString(","))
      .option("cubeSize", 5000)
      .save(tmpDir)

    ConvertToQbeastCommand(s"parquet.`$tmpDir`", columnsToIndex, 5000)
    spark.read.parquet(tmpDir).count shouldBe dataSize
  })

  it should "throw an exception when attempting to convert an unsupported format" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val df = loadTestData(spark)
      df.write.mode("overwrite").json(tmpDir)

      an[AnalysisException] shouldBe thrownBy(
        ConvertToQbeastCommand(s"json.`$tmpDir`", columnsToIndex).run(spark))
    })

  "Analyzing the conversion revision" should "do nothing" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      convertFormatsFromTo(spark, "parquet", tmpDir)
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.analyze().isEmpty shouldBe true
    })

  "Optimizing the conversion revision" should "do nothing" in
    withSparkAndTmpDir((spark, tmpDir) => {
      convertFormatsFromTo(spark, "parquet", tmpDir)
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.analyze()
      qbeastTable.optimize()

      val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
      spark.read.parquet(tmpDir).count shouldBe dataSize
      metrics.cubeCount shouldBe 0
      metrics.elementCount shouldBe 0
    })

  it should "preserve sampling accuracy" in withSparkAndTmpDir((spark, tmpDir) => {
    convertFormatsFromTo(spark, "parquet", tmpDir)
    val qbeastTable = QbeastTable.forPath(spark, tmpDir)

    qbeastTable.analyze()
    qbeastTable.optimize()

    val convertedTable = spark.read.format("qbeast").load(tmpDir)
    val tolerance = 0.01
    List(0.1, 0.2, 0.5, 0.7, 0.99).foreach(f => {
      val sampleSize = convertedTable
        .sample(withReplacement = false, f)
        .count()
        .toDouble

      val margin = dataSize * f * tolerance
      sampleSize shouldBe (dataSize * f) +- margin
    })
  })

  "Compacting the conversion revision" should "do nothing" in
    withExtendedSparkAndTmpDir(
      sparkConfWithSqlAndCatalog
        .set("spark.qbeast.compact.minFileSize", "1")
        .set("spark.qbeast.compact.maxFileSize", "2000000")) { (spark, tmpDir) =>
      {
        convertFormatsFromTo(spark, "parquet", tmpDir)
        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.compact()

        val metrics = QbeastTable.forPath(spark, tmpDir).getIndexMetrics()
        spark.read.parquet(tmpDir).count shouldBe dataSize
        metrics.cubeCount shouldBe 0
        metrics.elementCount shouldBe 0
      }
    }
}
