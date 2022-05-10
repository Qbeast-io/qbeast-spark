package io.qbeast.spark.utils

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import org.apache.spark.sql.AnalysisException

class ConvertToQbeastTest extends QbeastIntegrationTestSpec {

  "ConvertToQbeast" should "convert a Parquet Table to a Qbeast Table" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = loadTestData(spark)
      df.write.mode("overwrite").parquet(tmpDir)
      val columnsToIndex = Seq("user_id", "product_id")

      // Run the command
      ConvertToQbeastCommand(tmpDir, "parquet", columnsToIndex).run(spark)

      val indexed = spark.read.format("qbeast").load(tmpDir)
      indexed.count shouldBe df.count

    })

  it should "convert a Delta Table to a Qbeast Table" in withSparkAndTmpDir((spark, tmpDir) => {
    val df = loadTestData(spark)
    df.write.format("delta").save(tmpDir)
    val columnsToIndex = Seq("user_id", "product_id")

    // Run the command
    ConvertToQbeastCommand(tmpDir, "delta", columnsToIndex).run(spark)

    val indexed = spark.read.format("qbeast").load(tmpDir)
    indexed.count shouldBe df.count

  })

  it should "throw an error when converting another file format" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = loadTestData(spark)
      df.write.mode("overwrite").json(tmpDir)
      val columnsToIndex = Seq("user_id", "product_id")

      // Run the command
      an[UnsupportedOperationException] shouldBe thrownBy(
        ConvertToQbeastCommand(tmpDir, "json", columnsToIndex)
          .run(spark))
    })

  it should "throw an error when the file format does not match" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = loadTestData(spark)
      // write as json
      df.write.mode("overwrite").json(tmpDir)
      val columnsToIndex = Seq("user_id", "product_id")

      // Run the command
      // read as delta
      an[AnalysisException] shouldBe thrownBy(
        ConvertToQbeastCommand(tmpDir, "delta", columnsToIndex)
          .run(spark))

    })
}
