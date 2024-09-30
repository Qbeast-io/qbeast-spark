/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.CubeStatus
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.QTableId
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.AppendedClues.convertToClueful

class QbeastSnapshotTest extends QbeastIntegrationTestSpec {

  def createDF(size: Int): Dataset[Client3] = {
    val spark = SparkSession.active
    import spark.implicits._

    spark
      .range(size)
      .map(i => Client3(i * i, s"student-$i", i.intValue(), i * 2, i * i))

  }

  "QbeastSnapshot" should
    "load last index status correctly" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val df = createDF(1000)
        val names = List("age", "val2")
        val cubeSize = 10
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
        val indexStatus = qbeastSnapshot.loadLatestIndexStatus
        val revision = indexStatus.revision

        revision.revisionID shouldBe 1L
        qbeastSnapshot.loadIndexStatus(revision.revisionID) shouldBe indexStatus
        val latestRevisionID = qbeastSnapshot.loadLatestRevision.revisionID
        qbeastSnapshot.loadIndexStatus(latestRevisionID) shouldBe indexStatus
      }
    }

  it should "load last revision correctly" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val df = createDF(1000)
        val names = List("age", "val2")
        val cubeSize = 10
        val options =
          Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
        val columnTransformers = SparkRevisionFactory
          .createNewRevision(QTableId(tmpDir), df.schema, QbeastOptions(options))
          .columnTransformers

        val revision = qbeastSnapshot.loadLatestRevision
        revision.revisionID shouldBe 1L
        revision.tableId shouldBe QTableId(tmpDir)
        revision.columnTransformers.map(_.columnName) shouldBe names
        revision.desiredCubeSize shouldBe cubeSize
        revision.columnTransformers shouldBe columnTransformers

      }
    }

  it should "load revision at certain timestamp" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val df = createDF(1000)
        val names = List("age", "val2")
        val cubeSize = 10
        val options =
          Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)
        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
        val timestamp = System.currentTimeMillis()
        qbeastSnapshot.loadRevisionAt(timestamp) shouldBe qbeastSnapshot.loadLatestRevision

      }
    }

  it should "throw an exception when no revision satisfy timestamp requirement" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        val invalidRevisionTimestamp = System.currentTimeMillis()

        val df = createDF(1000)
        val names = List("age", "val2")
        val cubeSize = 10
        val options =
          Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
        an[AnalysisException] shouldBe thrownBy(
          qbeastSnapshot.loadRevisionAt(invalidRevisionTimestamp))

      }
    }

  "Overflowed set" should
    "contain only cubes that surpass desiredCubeSize" in withQbeastContextSparkAndTmpDir {
      (spark, tmpDir) =>
        {

          val df = createDF(100000)
          val names = List("age", "val2")
          val cubeSize = 10000
          df.write
            .format("qbeast")
            .mode("overwrite")
            .options(
              Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString))
            .save(tmpDir)

          val deltaLog = DeltaLog.forTable(spark, tmpDir)
          val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
          val builder =
            new IndexStatusBuilder(
              qbeastSnapshot,
              qbeastSnapshot.loadLatestIndexStatus.revision,
              Set.empty)
          val revisionState = builder.indexCubeStatuses

          val overflowed = qbeastSnapshot.loadLatestIndexStatus.overflowedSet

          revisionState
            .filter { case (cube, _) => overflowed.contains(cube) }
            .foreach { case (cube, CubeStatus(_, weight, _, _, elementCount)) =>
              elementCount should be > (cubeSize * 0.9).toLong withClue
                "assertion failed in cube " + cube +
                " where size is " + size + " and weight is " + weight
            }
        }
    }

  "loadDataframeFromData" should " correct the a table from a list of files" in {
    withSparkAndTmpDir { (spark, tmpDir) =>
      val df = createDF(10000)
      val names = List("age", "val2")
      val cubeSize = 100
      val options =
        Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)
      df.write
        .format("qbeast")
        .mode("overwrite")
        .options(options)
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      import spark.implicits._

      val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
      val lastRev = qbeastSnapshot.loadLatestRevision
      val files = qbeastSnapshot.loadRevisionFiles(lastRev.revisionID)
      val cubeStatus = qbeastSnapshot.loadIndexStatus(lastRev.revisionID).cubesStatuses
      cubeStatus.size should be > 50
      val filesToOptim =
        files.map(IndexFiles.fromAddFile(lastRev.columnTransformers.size))
      val allData = qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptim)
      allData.count.toInt shouldBe 10000

      import spark.implicits._
      val data = spark.read.format("qbeast").load(tmpDir)
      val (filePath, fileSize) =
        data.groupBy(input_file_name()).count().as[(String, Long)].first()
      val fileName = new Path(filePath).getName
      val subSet =
        qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptim.filter(_.path == fileName))

      subSet.count shouldBe fileSize

    }
  }

  "loadDataframeFromIndexFiles" should "work properly with evolving schema" in {
    withSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._

      val cubeSize = 100
      val options =
        Map("columnsToIndex" -> "id", "cubeSize" -> cubeSize.toString)
      spark
        .range(100)
        .filter("id % 2 = 0")
        .write
        .format("qbeast")
        .mode("overwrite")
        .options(options)
        .save(tmpDir)

      spark
        .range(100)
        .filter("id % 2 = 1")
        .withColumn("rand", rand())
        .write
        .option("mergeSchema", "true")
        .format("qbeast")
        .mode("append")
        .options(options)
        .save(tmpDir)

      spark.read.format("qbeast").load(tmpDir).count.toInt shouldBe 100

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

      val lastRev = qbeastSnapshot.loadLatestRevision
      val files = qbeastSnapshot.loadAllRevisions
        .map(rev => qbeastSnapshot.loadRevisionFiles(rev.revisionID))
        .foldLeft(spark.emptyDataset[AddFile])(_ union _)
      import spark.implicits._
      val filesToOptimize: Dataset[IndexFile] =
        files.map(IndexFiles.fromAddFile(lastRev.columnTransformers.size))
      val allData = qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptimize)
      allData.count.toInt shouldBe 100

      val data = spark.read.format("qbeast").load(tmpDir)
      // I'm selecting only the old file with the first schema with only 1 column
      val filePath =
        data.filter("id % 2 = 0").select(input_file_name()).distinct().as[String].first()

      spark.read.parquet(filePath).columns shouldBe Seq("id")

      val fileName = new Path(filePath).getName

      val subSet =
        qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptimize.filter(_.path == fileName))

      subSet.schema shouldBe data.schema
      subSet.columns shouldBe Seq("id", "rand")

    }
  }

  it should "fail if deletion Vector are enable" in {
    withExtendedSparkAndTmpDir(
      sparkConfWithSqlAndCatalog
        .set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")) {
      (spark, tmpDir) =>
        val nrows = 100
        val df = createDF(nrows)
        val names = List("age", "val2")
        val cubeSize = 100
        val options =
          Map("columnsToIndex" -> names.mkString(","), "cubeSize" -> cubeSize.toString)
        df.write
          .format("qbeast")
          .option("enableDeletionVectors", "true")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)
        val deltaLog = DeltaLog.forTable(spark, tmpDir)
        val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
        val lastRev = qbeastSnapshot.loadLatestRevision
        val files = qbeastSnapshot.loadRevisionFiles(lastRev.revisionID)

        import spark.implicits._
        val filesToOptim =
          files.map(IndexFiles.fromAddFile(lastRev.columnTransformers.size))

        intercept[UnsupportedOperationException] {
          qbeastSnapshot.loadDataframeFromIndexFiles(filesToOptim)
        }

    }
  }

}
