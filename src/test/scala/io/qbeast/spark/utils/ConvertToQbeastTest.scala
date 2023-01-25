package io.qbeast.spark.utils

import io.qbeast.core.model.RevisionUtils.{isStaging, stagingID}
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.scalatest.PrivateMethodTester

class ConvertToQbeastTest extends QbeastIntegrationTestSpec with PrivateMethodTester {
  val columnsToIndex: Seq[String] = Seq("user_id", "price", "event_type")
  val partitionColumns: Option[String] = Some("event_type STRING")
  val numSparkPartitions = 20
  val dataSize = 50000
  val dcs = 5000

  def convertFromFormat(
      spark: SparkSession,
      format: String,
      tablePath: String,
      partitionColumns: Option[String] = None,
      columnsToIndex: Seq[String] = columnsToIndex,
      desiredCubeSize: Int = dcs): DataFrame = {
    val data = loadTestData(spark).limit(dataSize).repartition(numSparkPartitions)

    if (partitionColumns.isDefined) {
      val cols = partitionColumns.get.split(", ").map(_.split(" ").head)
      data.write.mode("overwrite").partitionBy(cols: _*).format(format).save(tablePath)
    } else data.write.mode("overwrite").format(format).save(tablePath)

    val tableIdentifier = s"$format.`$tablePath`"
    ConvertToQbeastCommand(tableIdentifier, columnsToIndex, desiredCubeSize, partitionColumns)
      .run(spark)

    spark.read.format("qbeast").load(tablePath)
  }

  def getQbeastSnapshot(spark: SparkSession, dir: String): DeltaQbeastSnapshot = {
    val deltaLog = DeltaLog.forTable(spark, dir)
    DeltaQbeastSnapshot(deltaLog.snapshot)
  }

  "ConvertToQbeastCommand" should "convert a delta table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFromFormat(spark, "delta", tmpDir)

      convertedTable.count shouldBe dataSize
      spark.read.parquet(tmpDir).count shouldBe dataSize

      val indexStatus = getQbeastSnapshot(spark, tmpDir).loadIndexStatus(stagingID)
      // All non-qbeast files are considered staging files and are placed
      // directly into the staging revision(RevisionID = 0)
      indexStatus.cubesStatuses.size shouldBe 1
      indexStatus.cubesStatuses.head._2.files.size shouldBe numSparkPartitions
    })

  it should "convert a PARTITIONED delta table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFromFormat(spark, "delta", tmpDir, partitionColumns)

      convertedTable.count shouldBe dataSize
      spark.read.parquet(tmpDir).count shouldBe dataSize

      val indexStatus = getQbeastSnapshot(spark, tmpDir).loadIndexStatus(stagingID)
      indexStatus.cubesStatuses.size shouldBe 1
    })

  it should "convert a parquet table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFromFormat(spark, "parquet", tmpDir)

      convertedTable.count shouldBe dataSize
      spark.read.parquet(tmpDir).count shouldBe dataSize

      val indexStatus = getQbeastSnapshot(spark, tmpDir).loadIndexStatus(stagingID)
      indexStatus.cubesStatuses.size shouldBe 1
      indexStatus.cubesStatuses.head._2.files.size shouldBe numSparkPartitions
    })

  it should "convert a PARTITIONED parquet table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFromFormat(spark, "parquet", tmpDir, partitionColumns)

      convertedTable.count shouldBe dataSize
      spark.read.parquet(tmpDir).count shouldBe dataSize

      val indexStatus = getQbeastSnapshot(spark, tmpDir).loadIndexStatus(stagingID)
      indexStatus.cubesStatuses.size shouldBe 1
    })

  it should "not change a qbeast table" in withSparkAndTmpDir((spark, tmpDir) => {
    val data = loadTestData(spark).limit(dataSize)
    data.write
      .format("qbeast")
      .option("columnsToIndex", columnsToIndex.mkString(","))
      .option("cubeSize", dcs)
      .save(tmpDir)

    val revisionsBefore = getQbeastSnapshot(spark, tmpDir).loadAllRevisions

    ConvertToQbeastCommand(s"parquet.`$tmpDir`", columnsToIndex, dcs)

    val revisionsAfter = getQbeastSnapshot(spark, tmpDir).loadAllRevisions

    spark.read.parquet(tmpDir).count shouldBe dataSize
    revisionsAfter shouldBe revisionsBefore
  })

  it should "throw an exception when attempting to convert an unsupported format" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val df = loadTestData(spark)
      df.write.mode("overwrite").json(tmpDir)

      an[AnalysisException] shouldBe thrownBy(
        ConvertToQbeastCommand(s"json.`$tmpDir`", columnsToIndex).run(spark))
    })

  it should "preserve sampling accuracy" in withSparkAndTmpDir((spark, tmpDir) => {
    convertFromFormat(spark, "parquet", tmpDir)
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

  "Appending to a converted table" should "create a new, non-staging revision" in
    withSparkAndTmpDir((spark, tmpDir) => {
      convertFromFormat(spark, "parquet", tmpDir)
      val df = loadTestData(spark).limit(dataSize)
      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", columnsToIndex.mkString(","))
        .option("cubeSize", dcs)
        .save(tmpDir)

      val qs = getQbeastSnapshot(spark, tmpDir)
      qs.loadAllRevisions.size shouldBe 2

      val rev = qs.loadLatestRevision
      isStaging(rev) shouldBe false
    })

  "Analyzing the staging revision" should "not change the ANNOUNCED set" in
    withSparkAndTmpDir((spark, tmpDir) => {
      convertFromFormat(spark, "parquet", tmpDir)
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)
      qbeastTable.analyze()

      val qs = getQbeastSnapshot(spark, tmpDir)
      qs.loadLatestIndexStatus.announcedSet.isEmpty shouldBe true
    })

  "Optimizing the staging revision" should "not replicate any data" in
    withSparkAndTmpDir((spark, tmpDir) => {
      convertFromFormat(spark, "parquet", tmpDir)
      val qbeastTable = QbeastTable.forPath(spark, tmpDir)

      qbeastTable.analyze()
      qbeastTable.optimize()

      spark.read.parquet(tmpDir).count shouldBe dataSize
    })

  "Compacting the staging revision" should "reduce the number of delta AddFiles" in
    withExtendedSparkAndTmpDir(
      sparkConfWithSqlAndCatalog
        .set("spark.qbeast.compact.minFileSize", "1")
        .set("spark.qbeast.compact.maxFileSize", "2000000")) { (spark, tmpDir) =>
      {
        convertFromFormat(spark, "parquet", tmpDir)
        val qbeastTable = QbeastTable.forPath(spark, tmpDir)
        qbeastTable.compact()

        spark.read.parquet(tmpDir).count shouldBe >(dataSize.toLong)

        val qs = getQbeastSnapshot(spark, tmpDir)
        val stagingCs = qs.loadLatestIndexStatus.cubesStatuses

        stagingCs.size shouldBe 1
        stagingCs.head._2.files.size shouldBe <(numSparkPartitions)
      }
    }
}
