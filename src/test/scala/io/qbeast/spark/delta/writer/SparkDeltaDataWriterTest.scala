package io.qbeast.spark.delta.writer

import io.qbeast.TestClasses._
import io.qbeast.core.model.{CubeId, IndexStatus, QTableID, Weight}
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.spark.index.{SparkOTreeManager, SparkRevisionFactory}
import io.qbeast.spark.utils.{TagUtils}
import org.apache.spark.qbeast.config.{
  MIN_COMPACTION_FILE_SIZE_IN_BYTES,
  MAX_COMPACTION_FILE_SIZE_IN_BYTES
}
import org.apache.spark.sql.delta.actions.AddFile

import scala.reflect.io.Path

class SparkDeltaDataWriterTest extends QbeastIntegrationTestSpec {

  "SparkDataWriter" should "write the data correctly" in
    withSparkAndTmpDir { (spark, tmpDir) =>
      val cubeSize = 1000
      val size = 10000
      val df = spark.createDataFrame(spark.sparkContext.parallelize(0
        .to(size)
        .map(i =>
          Client4(i * i, s"student-$i", Some(i), Some(i * 1000 + 123), Some(i * 2567.3432143)))))

      val tableID = QTableID(tmpDir)
      val parameters: Map[String, String] =
        Map("columnsToIndex" -> "age,val2", "cubeSize" -> cubeSize.toString)
      val indexStatus =
        IndexStatus(SparkRevisionFactory.createNewRevision(tableID, df.schema, parameters))
      val (qbeastData, tableChanges) = SparkOTreeManager.index(df, indexStatus)

      val fileActions = SparkDeltaDataWriter.write(tableID, df.schema, qbeastData, tableChanges)

      for (fa <- fileActions) {
        Path(tmpDir + "/" + fa.path).exists shouldBe true
        fa.dataChange shouldBe true
      }
    }

  "Compactor process" should "organize files into groups of " +
    "[MIN_FILE_SIZE, MAX_FILE_SIZE]" in withSparkAndTmpDir { (spark, tmpDir) =>
      val writeTestSpec = WriteTestSpec(10, spark, tmpDir)
      writeTestSpec.writeData()

      val cubeFiles = writeTestSpec.cubeStatuses.mapValues(_.files).toIndexedSeq
      val groupedCubeFiles = SparkDeltaDataWriter.groupFilesToCompact(cubeFiles)

      val groupedFiles = groupedCubeFiles.map(_._2)

      groupedFiles.foreach(blocks => {
        val size = blocks.map(_.size).sum
        size shouldBe >=(MIN_COMPACTION_FILE_SIZE_IN_BYTES)
        size shouldBe <=(MAX_COMPACTION_FILE_SIZE_IN_BYTES)
      })

    }

  it should "compute correctly the final element count of the files" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val writeTestSpec = WriteTestSpec(10, spark, tmpDir)
      writeTestSpec.writeData()

      val cubeFiles = writeTestSpec.cubeStatuses.mapValues(_.files).toIndexedSeq
      val groupedCubeFiles = SparkDeltaDataWriter.groupFilesToCompact(cubeFiles)
      val groupedCubeFilesCount =
        groupedCubeFiles.map(g => (g._1.string, g._2.map(_.elementCount).sum)).toMap

      val compactedFiles = SparkDeltaDataWriter.compact(
        writeTestSpec.tableID,
        writeTestSpec.data.schema,
        writeTestSpec.indexStatus,
        writeTestSpec.tableChanges)

      val addedFiles = compactedFiles.collect { case a: AddFile => a }
      addedFiles.foreach(a => {
        val path = writeTestSpec.tableID.id + "/" + a.path
        val cube = a.tags(TagUtils.cube)
        val realElementCount = spark.read.parquet(path).count()
        val tagElementCount = a.tags(TagUtils.elementCount).toLong
        tagElementCount shouldBe realElementCount
        tagElementCount shouldBe groupedCubeFilesCount(cube)
      })

  }

  it should "compute correctly the final weight of the files" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val writeTestSpec = WriteTestSpec(10, spark, tmpDir)
      writeTestSpec.writeData()

      val compactedFiles = SparkDeltaDataWriter.compact(
        writeTestSpec.tableID,
        writeTestSpec.data.schema,
        writeTestSpec.indexStatus,
        writeTestSpec.tableChanges)

      val addedFiles = compactedFiles.collect { case a: AddFile => a }
      addedFiles
        .groupBy(_.tags(TagUtils.cube))
        .foreach(groupCubeFiles => {
          val cube = CubeId(1, groupCubeFiles._1)
          val tagMaxWeight =
            groupCubeFiles._2.map(a => Weight(a.tags(TagUtils.maxWeight).toInt)).min
          val realMaxWeight = writeTestSpec.weightMap.get(cube)

          realMaxWeight shouldBe defined
          tagMaxWeight.fraction shouldBe realMaxWeight.get

        })
  }

  it should "compute correctly the final state of the files" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      val writeTestSpec = WriteTestSpec(10, spark, tmpDir)
      writeTestSpec.writeData()

      val compactedFiles = SparkDeltaDataWriter.compact(
        writeTestSpec.tableID,
        writeTestSpec.data.schema,
        writeTestSpec.indexStatus,
        writeTestSpec.tableChanges)

      val addedFiles = compactedFiles.collect { case a: AddFile => a }
      addedFiles
        .foreach(a => {
          val cube = CubeId(1, a.tags(TagUtils.cube))
          val realState = {
            if (writeTestSpec.replicatedSet.contains(cube)) "REPLICATED"
            else if (writeTestSpec.announcedSet.contains(cube)) "ANNOUNCED"
            else "FLOODED"
          }
          val tagState = a.tags(TagUtils.state)
          tagState shouldBe realState

        })
  }

}
