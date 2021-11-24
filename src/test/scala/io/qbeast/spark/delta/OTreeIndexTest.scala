package io.qbeast.spark.delta

import io.qbeast.TestClasses.T2
import io.qbeast.model.{AllSpace, Weight, WeightRange}
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

  it should "findSampleFiles" in withSparkAndTmpDir((spark, tmpdir) => {

    val source = createDF(1000, spark)

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", 10)
      .save(tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val tahoeFileIndex = {
      TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, deltaLog.snapshot, Seq.empty, false)
    }
    val oTreeIndex = OTreeIndex(tahoeFileIndex)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.snapshot)

    val allFiles = deltaLog.snapshot.allFiles.collect()

    val revision = qbeastSnapshot.loadLatestRevision
    val dimensionCount = revision.columnTransformers.length

    val querySpace = AllSpace(dimensionCount)
    val startCube = revision.createCubeIdRoot()
    val weightRange = WeightRange(Weight.MinValue, Weight.MaxValue)

    val b = new IndexStatusBuilder(qbeastSnapshot, qbeastSnapshot.loadLatestRevision, Set.empty)
    val indexStatus = b.build()
    val matchFiles = oTreeIndex.findSampleFiles(
      querySpace,
      weightRange,
      startCube,
      indexStatus.cubesStatuses,
      Set.empty,
      allFiles)

    val diff = (allFiles.toSet -- matchFiles.toSet)

    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "inputFiles" in withSparkAndTmpDir((spark, tmpdir) => {

    val source = createDF(1000, spark)

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", 10)
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

      val source = createDF(1000, spark)

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", 10)
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

    val source = createDF(1000, spark)

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,c")
      .option("cubeSize", 10)
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
