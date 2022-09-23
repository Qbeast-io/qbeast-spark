package io.qbeast.spark.utils

import io.qbeast.core.model.QTableID
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.{QbeastIntegrationTestSpec, QbeastTable}
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import org.apache.spark.SparkException
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.PrivateMethodTester
import org.apache.spark.sql.functions.col

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

    ConvertToQbeastCommand(dir, columnsToIndex).run(spark)

    spark.read.format(readFormat).load(dir)
  }

  "ConvertToQbeastCommand" should "convert a delta table into a qbeast table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFormatsFromTo("delta", "qbeast", spark, tmpDir)

      convertedTable.count shouldBe dataSize
    })

  it should "convert a PARTITIONED delta table into a qbeast table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      // Use a reduced dataset since partitionBy is more expensive to run
      val data = loadTestData(spark).limit(1000)
      val partitionColumns = Seq("event_type")
      data.write
        .mode("overwrite")
        .partitionBy(partitionColumns: _*)
        .format("delta")
        .save(tmpDir)

      // Convert a partitioned delta table to a qbeast table
      ConvertToQbeastCommand(tmpDir, columnsToIndex, partitionColumns = partitionColumns)
        .run(spark)

      val convertedTable =
        spark.read.format("qbeast").load(tmpDir)

      convertedTable.count shouldBe 1000
    })

  it should "convert a parquet table into a qbeast table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val convertedTable = convertFormatsFromTo("parquet", "qbeast", spark, tmpDir)

      convertedTable.count shouldBe dataSize
    })

  it should "convert a PARTITIONED parquet table into a qbeast table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      // Use a reduced dataset since partitionBy is more expensive to run
      val data = loadTestData(spark).limit(1000)
      val partitionColumns = Seq("event_type")
      data.write
        .mode("overwrite")
        .partitionBy(partitionColumns: _*)
        .format("parquet")
        .save(tmpDir)

      // Conversion: Partitioned parquet -> delta -> qbeast
      ConvertToQbeastCommand(tmpDir, columnsToIndex, partitionColumns = partitionColumns)
        .run(spark)

      val convertedTable =
        spark.read.format("qbeast").load(tmpDir)

      convertedTable.count shouldBe 1000
    })

  it should "throw an error when attempting to convert an unsupported format" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = loadTestData(spark)
      df.write.mode("overwrite").json(tmpDir)

      an[SparkException] shouldBe thrownBy(
        ConvertToQbeastCommand(tmpDir, columnsToIndex).run(spark))
    })

  it should "throw an error if columnsToIndex are not found in table schema" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val nonExistentColumns = Seq("a", "b")

      an[RuntimeException] shouldBe thrownBy(
        convertFormatsFromTo("delta", "qbeast", spark, tmpDir, nonExistentColumns))
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

  "ConvertToQbeastCommand's idempotence" should "not try to convert a converted table" in
    withSparkAndTmpDir((spark, tmpDir) => {
      // csv -> parquet -> delta -> qbeast
      convertFormatsFromTo("parquet", "qbeast", spark, tmpDir)
      // qbeast -> qbeast
      ConvertToQbeastCommand(tmpDir, columnsToIndex).run(spark)

      val convertedTable = spark.read.format("qbeast").load(tmpDir)
      val deltaLog = DeltaLog.forTable(spark, tmpDir)

      convertedTable.count shouldBe dataSize
      // Converting parquet to delta creates snapshot version 0, and its
      // conversion to qbeast creates snapshot version 1. If the second
      // conversion gets executed, it'd produce a snapshot version 2
      deltaLog.snapshot.version shouldBe 1
    })

  "A converted Delta table" should "be readable using delta" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val convertedTable = convertFormatsFromTo("delta", "delta", spark, tmpDir)
      convertedTable.count shouldBe dataSize
    })

  it should "be readable using parquet" in withSparkAndTmpDir((spark, tmpDir) => {
    val convertedTable = convertFormatsFromTo("delta", "parquet", spark, tmpDir)
    convertedTable.count shouldBe dataSize
  })

  "A converted parquet table" should "be readable using parquet" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val convertedTable = convertFormatsFromTo("parquet", "parquet", spark, tmpDir)
      convertedTable.count shouldBe dataSize
    })

  it should "be readable using delta" in withSparkAndTmpDir((spark, tmpDir) => {
    val convertedTable = convertFormatsFromTo("parquet", "delta", spark, tmpDir)
    convertedTable.count shouldBe dataSize
  })

  "extractQbeastTag" should
    "extract elementCount from file metadata if AddFile has corrupted stats" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val data = loadTestData(spark).limit(500)
      data
        .coalesce(1)
        .write
        .mode("overwrite")
        .format("delta")
        .save(tmpDir)

      val snapshot = DeltaLog.forTable(spark, tmpDir).snapshot
      val qbeastTag = ConvertToQbeastCommand.extractQbeastTag(
        snapshot.allFiles.first().copy(stats = "{this is a corrupt stats string}"),
        SparkRevisionFactory.createNewRevision(
          QTableID(tmpDir),
          snapshot.schema,
          Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> "5000000")),
        tmpDir)

      val countFromMetadata = qbeastTag(TagUtils.elementCount).toInt
      countFromMetadata shouldBe 500
    })

  "dataTypeToName" should
    "be able to convert data types: Int, Double, and Long" in withSparkAndTmpDir(
      (spark, tmpDir) => {
        val data = loadTestData(spark).limit(20)
        val partitionedData = data
          .withColumn("stringType", col("event_type"))
          .withColumn("integerType", col("user_id").cast("INT"))
          .withColumn("doubleType", col("price").cast("DOUBLE"))
          .withColumn("longType", col("category_id"))
          .withColumn("dateType", col("event_time").cast("DATE"))
          .drop(data.columns: _*)

        // integerType, doubleType, longType, dataType. Can't all columns for partitioning
        val partitionColumns = partitionedData.columns.tail
        partitionedData.write
          .mode("overwrite")
          .partitionBy(partitionColumns: _*)
          .parquet(tmpDir)

        // Partitioned parquet -> delta -> qbeast
        ConvertToQbeastCommand(
          tmpDir,
          Seq("doubleType", "integerType", "longType"),
          partitionColumns = partitionColumns)
          .run(spark)

        val convertedTable = spark.read.format("qbeast").load(tmpDir)
        convertedTable.count shouldBe 20
      })
}
