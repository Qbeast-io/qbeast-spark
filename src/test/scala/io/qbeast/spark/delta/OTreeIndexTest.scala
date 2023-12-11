package io.qbeast.spark.delta

import io.qbeast.core.model.Block
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.T2
import io.qbeast.spark.internal.commands.ConvertToQbeastCommand
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

class OTreeIndexTest extends QbeastIntegrationTestSpec {

  class OTreeIndexTest(tahoe: TahoeLogFileIndex) extends OTreeIndex(index = tahoe) {

    // Testing protected method
    override def matchingBlocks(
        partitionFilters: Seq[Expression],
        dataFilters: Seq[Expression]): Iterable[Block] =
      super.matchingBlocks(partitionFilters, dataFilters)

  }

  private def createDF(size: Int, spark: SparkSession) = {
    import spark.implicits._

    0.to(size)
      .map(i => T2(i, i.toDouble))
      .toDF()
      .as[T2]

  }

  behavior of "OTreeIndexTest"

  it should "find all matching files" in withSparkAndTmpDir((spark, tmpdir) => {

    val source = createDF(100000, spark)

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", 1000)
      .save(tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val tahoeFileIndex = {
      TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.update(), Seq.empty, false)
    }
    val oTreeIndex = new OTreeIndexTest(tahoeFileIndex)

    val allFiles = deltaLog.update().allFiles.collect().map(_.path)

    val matchFiles = oTreeIndex.matchingBlocks(Seq.empty, Seq.empty).map(_.file.path).toSet

    val diff = (allFiles.toSet -- matchFiles.toSet)

    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "inputFiles" in withSparkAndTmpDir((spark, tmpdir) => {

    val source = createDF(10000, spark)

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", 1000)
      .save(tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val tahoeFileIndex = {
      TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.update(), Seq.empty, false)
    }
    val oTreeIndex = new OTreeIndexTest(tahoeFileIndex)

    oTreeIndex.inputFiles shouldBe deltaLog
      .update()
      .allFiles
      .collect()
      .map(file => new Path(deltaLog.dataPath, file.path).toString)
  })

  it should "filter correctly the files in matchingFiles" in withSparkAndTmpDir(
    (spark, tmpdir) => {

      val source = createDF(10000, spark)

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", 1000)
        .save(tmpdir)

      val deltaLog = DeltaLog.forTable(spark, tmpdir)
      val tahoeFileIndex = {
        TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.update(), Seq.empty, false)
      }
      val oTreeIndex = new OTreeIndexTest(tahoeFileIndex)
      val allFiles = deltaLog.update().allFiles.collect().map(_.path)

      oTreeIndex
        .matchingBlocks(Seq.empty, Seq.empty)
        .map(_.file.path)
        .toSet shouldBe allFiles.toSet
    })

  it should "sizeInBytes" in withSparkAndTmpDir((spark, tmpdir) => {

    val source = createDF(10000, spark)

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", 1000)
      .save(tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val tahoeFileIndex = {
      TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.update(), Seq.empty, false)
    }
    val oTreeIndex = new OTreeIndexTest(tahoeFileIndex)

    val sizeInBytes = deltaLog.update().allFiles.collect().map(_.size).sum
    oTreeIndex.sizeInBytes shouldBe sizeInBytes
  })

  it should "filter files with underlying data skipping" in withSparkAndTmpDir(
    (spark, tmpdir) => {

      import spark.implicits._
      val source = Seq(1, 2, 3, 4).toDF("id")

      source
        .coalesce(4)
        .write
        .format("delta")
        .save(tmpdir)

      // CONVERT TO QBEAST
      ConvertToQbeastCommand.apply(tmpdir, Seq("id"), 1000)

      val deltaLog = DeltaLog.forTable(spark, tmpdir)
      val snapshot = deltaLog.update()
      val tahoeFileIndex =
        TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, snapshot, Seq.empty, false)
      val oTreeIndex = new OTreeIndexTest(tahoeFileIndex)

      val allFiles = snapshot.allFiles
      val filteredFiles = oTreeIndex.listFiles(Seq.empty, Seq(expr("id == 1").expr))

      allFiles.count() shouldBe 4
      filteredFiles.size shouldBe 1
    })

}
