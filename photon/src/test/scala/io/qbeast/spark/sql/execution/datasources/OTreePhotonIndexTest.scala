package io.qbeast.spark.sql.execution.datasources

import io.qbeast.spark.sql.connector.catalog.QbeastTestSpec
import io.qbeast.spark.sql.execution.SampleOperator
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.types.StructType

class OTreePhotonIndexTest extends QbeastTestSpec {

  behavior of "OTreePhotonIndexTest"

  it should "return the sizeInBytes" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val oTreePhotonIndex = OTreePhotonIndex(spark, snapshot, Map.empty)

    val sizeInBytes = spark.read
      .format("parquet")
      .load(qbeastDataPath)
      .queryExecution
      .optimizedPlan
      .stats
      .sizeInBytes
    oTreePhotonIndex.sizeInBytes shouldBe sizeInBytes
  })

  it should "return the list of inputFiles" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val oTreePhotonIndex = OTreePhotonIndex(spark, snapshot, Map.empty)

    oTreePhotonIndex.inputFiles.length shouldBe 9
  })

  it should "return empty partitionSpec" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val oTreePhotonIndex = OTreePhotonIndex(spark, snapshot, Map.empty)

    oTreePhotonIndex.partitionSpec() shouldBe PartitionSpec(StructType(Seq.empty), Seq.empty)
  })

  it should "return single rootPath" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val oTreePhotonIndex = OTreePhotonIndex(spark, snapshot, Map.empty)

    oTreePhotonIndex.rootPaths.length shouldBe 1
    oTreePhotonIndex.rootPaths.head shouldBe new Path(qbeastDataPath)
  })

  it should "return listFiles" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val oTreePhotonIndex = OTreePhotonIndex(spark, snapshot, Map.empty)

    oTreePhotonIndex.listFiles(Seq.empty, Seq.empty).flatMap(_.files) shouldBe oTreePhotonIndex
      .allFiles()
  })

  it should "filter files when sampling is pushed down" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val oTreePhotonIndex = OTreePhotonIndex(spark, snapshot, Map.empty)

    // We pushdown the sample operator
    oTreePhotonIndex.pushdownSample(SampleOperator(0.0, 0.1, withReplacement = true, 42))

    oTreePhotonIndex.listFiles(Seq.empty, Seq.empty).size shouldBe <(
      oTreePhotonIndex.allFiles().size)
  })

  it should "return empty partitionSchema" in withSpark(spark => {

    val snapshot = QbeastPhotonSnapshot(spark, qbeastDataPath)
    val oTreePhotonIndex = OTreePhotonIndex(spark, snapshot, Map.empty)

    oTreePhotonIndex.partitionSchema shouldBe StructType(Seq.empty)
  })

}
