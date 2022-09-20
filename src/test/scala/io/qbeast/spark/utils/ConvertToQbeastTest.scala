package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog

class ConvertToQbeastTest extends QbeastIntegrationTestSpec {
  val columnsToIndex: Seq[String] = Seq("user_id", "price", "category_id")

  def showFileLog(spark: SparkSession, path: String, truncate: Boolean = true): Unit = {
    // scalastyle:off println
    val snapshot = DeltaLog.forTable(spark, path).snapshot

    println("AddFiles:")
    snapshot.allFiles.show(truncate)

    println("RemoveFiles:")
    snapshot.tombstones.show(truncate)
  }

  "ConvertToQbeast" should "convert a Parquet Table to a Qbeast Table" in withSparkAndTmpDir(
    (spark, tmpDir) => {})

  it should "convert a Delta Table to a Qbeast Table" in withSparkAndTmpDir((spark, tmpDir) => {})

  it should "throw an error when attempting to convert to unsupported format" in withSparkAndTmpDir(
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

  "A converted delta table" should "be readable using delta" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = loadTestData(spark)
      df.write.format("delta").save(tmpDir)
//      showFileLog(spark, tmpDir)

      val fileFormat = "delta"
      ConvertToQbeastCommand(tmpDir, fileFormat, columnsToIndex).run(spark)
//      showFileLog(spark, tmpDir, truncate = false)

      spark.read.format("delta").load(tmpDir).count shouldBe df.count
    })

  it should "be readable using qbeast" in withSparkAndTmpDir((spark, tmpDir) => {
    val df = loadTestData(spark)
    val fileFormat = "delta"
    df.write.format(fileFormat).save(tmpDir)
//    showFileLog(spark, tmpDir)

    ConvertToQbeastCommand(tmpDir, fileFormat, columnsToIndex).run(spark)
//    showFileLog(spark, tmpDir, truncate = false)

    spark.read.format("qbeast").load(tmpDir).count shouldBe df.count
  })

}
