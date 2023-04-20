package io.qbeast.spark.sql.execution.datasources

import io.qbeast.spark.sql.connector.catalog.QbeastTestSpec
import org.apache.spark.sql.AnalysisException

class QbeastPhotonSnapshotTest extends QbeastTestSpec {

  behavior of "QbeastPhotonSnapshotTest"

  it should "loadRevisionAt" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)

    snapshot.loadRevisionAt(System.currentTimeMillis()) shouldBe snapshot.loadLatestRevision
  })

  it should "loadAllRevisions" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)

    snapshot.loadAllRevisions.length shouldBe 2
  })

  it should "throw an error when revision does not exists" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    a[AnalysisException] shouldBe thrownBy(snapshot.loadRevision(8))
  })

  it should "isInitial" in withSparkAndTmpDir((spark, tmpDir) => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    snapshot.isInitial shouldBe false

    val nonExistingSnapshot = QbeastPhotonSnapshot(spark, tmpDir)
    nonExistingSnapshot.isInitial shouldBe true
  })

  it should "loadLatestIndexStatus" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val loadIndexStatus = snapshot.loadLatestIndexStatus

    loadIndexStatus.revision shouldBe snapshot.loadLatestRevision
    loadIndexStatus.announcedSet shouldBe Set.empty
    loadIndexStatus.replicatedSet shouldBe Set.empty
    loadIndexStatus.cubesStatuses.size shouldBe 9
  })

  it should "loadIndexStatus" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)

    snapshot.loadIndexStatus(1) shouldBe snapshot.loadLatestIndexStatus
  })

  it should "loadLatestRevision" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val latestRevision = snapshot.loadLatestRevision

    latestRevision.revisionID shouldBe 1
    latestRevision.desiredCubeSize shouldBe 30000
    latestRevision.columnTransformers.length shouldBe 2
    latestRevision.transformations.length shouldBe 2
  })

  it should "loadRevision" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val loadRevision = snapshot.loadRevision(1)

    loadRevision shouldBe snapshot.loadLatestRevision
  })

}
