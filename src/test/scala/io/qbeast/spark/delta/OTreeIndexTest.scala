package io.qbeast.spark.delta

import io.qbeast.TestClasses.T2
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.files.TahoeLogFileIndex

class OTreeIndexTest extends QbeastIntegrationTestSpec {

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
      TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.snapshot, Seq.empty, false)
    }
    val oTreeIndex = OTreeIndex(tahoeFileIndex)

    val allFiles = deltaLog.snapshot.allFiles.collect()

    val matchFiles = oTreeIndex.matchingFiles(Seq.empty, Seq.empty)

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
      TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.snapshot, Seq.empty, false)
    }
    val oTreeIndex = OTreeIndex(tahoeFileIndex)

    oTreeIndex.inputFiles shouldBe deltaLog.snapshot.allFiles
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
        TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.snapshot, Seq.empty, false)
      }
      val oTreeIndex = OTreeIndex(tahoeFileIndex)
      val allFiles = deltaLog.snapshot.allFiles.collect()

      oTreeIndex.matchingFiles(Seq.empty, Seq.empty).toSet shouldBe allFiles.toSet
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
      TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.snapshot, Seq.empty, false)
    }
    val oTreeIndex = OTreeIndex(tahoeFileIndex)

    val sizeInBytes = deltaLog.snapshot.allFiles.collect().map(_.size).sum
    oTreeIndex.sizeInBytes shouldBe sizeInBytes
  })

}
