package io.qbeast.spark.delta

import io.qbeast.model.{AllSpace, Weight, WeightRange}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.T2
import io.qbeast.spark.utils.TagUtils
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.files.TahoeLogFileIndex

class OTreeIndexTest extends QbeastIntegrationTestSpec {

  // TODO: Finish these tests of OTreeIndex

  behavior of "OTreeIndexTest"

  it should "findSampleFiles" in withSparkAndTmpDir((spark, tmpdir) => {

    import spark.implicits._
    val source = 0
      .to(1000)
      .map(i => T2(i, i.toDouble))
      .toDF()
      .as[T2]

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
    val dimensionCount = 2
    val querySpace = AllSpace(dimensionCount)
    val startCube = qbeastSnapshot.loadLatestRevision.createCubeIdRoot()
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

    val cubes = allFiles.map(_.tags(TagUtils.cube)).toSet

    cubes.count(a =>
      if (a.nonEmpty) {
        // Am I an orphan?
        !cubes.contains(a.substring(0, a.length - 1))
      } else {
        // the root is never an orphan!
        false

      }) shouldBe 0

    //scalastyle:off
    (allFiles.toSet -- matchFiles.toSet).foreach(println)
    matchFiles.size shouldBe allFiles.size
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "inputFiles" in {}

  it should "matchingFiles" in {}

  it should "sizeInBytes" in {}

}
