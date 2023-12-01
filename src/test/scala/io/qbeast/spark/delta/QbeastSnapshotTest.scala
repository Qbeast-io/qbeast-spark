/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.CubeStatus
import io.qbeast.core.model.QTableID
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.AppendedClues.convertToClueful

class QbeastSnapshotTest extends QbeastIntegrationTestSpec {

  def createDF(size: Int): Dataset[Client3] = {
    val spark = SparkSession.active
    import spark.implicits._

    1.to(size)
      .map(i => Client3(i * i, s"student-$i", i, i * 2, i * i))
      .toDF()
      .as[Client3]
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
          .createNewRevision(QTableID(tmpDir), df.schema, options)
          .columnTransformers

        val revision = qbeastSnapshot.loadLatestRevision
        revision.revisionID shouldBe 1L
        revision.tableID shouldBe QTableID(tmpDir)
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
            .foreach { case (cube, CubeStatus(_, weight, _, files)) =>
              val size = files
                .map(a => a.elementCount)
                .sum

              size should be > (cubeSize * 0.9).toLong withClue
                "assertion failed in cube " + cube +
                " where size is " + size + " and weight is " + weight
            }
        }
    }

}
