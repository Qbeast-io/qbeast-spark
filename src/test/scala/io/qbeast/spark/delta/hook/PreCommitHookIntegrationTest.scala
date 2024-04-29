package io.qbeast.spark.delta.hook

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import io.qbeast.spark.delta.hook.PreCommitHook.PRE_COMMIT_HOOKS_PREFIX
import io.qbeast.spark.delta.hook.PreCommitHook.PreCommitHookOutput
import io.qbeast.spark.delta.hook.SimpleHook.testOutput
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.DeltaLog

private class SimpleHook extends PreCommitHook {
  override val name: String = "SimpleHook"

  override def run(args: Seq[Action]): PreCommitHookOutput = {
    testOutput
  }

}

private object SimpleHook {
  val testOutput: Map[String, String] = Map("clsName" -> "SimpleHook")
}

class PreCommitHookIntegrationTest extends QbeastIntegrationTestSpec {

  "PreCommitHook" should "run a simple hook and save its outputs to CommitInfo during writes" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._

      val df = spark.sparkContext.range(0, 10).toDF("id")
      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option(s"$PRE_COMMIT_HOOKS_PREFIX.hook", classOf[SimpleHook].getCanonicalName)
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val conf = deltaLog.newDeltaHadoopConf()

      val infoTags = deltaLog.store
        .read(FileNames.deltaFile(deltaLog.logPath, snapshot.version), conf)
        .map(Action.fromJson)
        .collect { case commitInfo: CommitInfo =>
          println(commitInfo)
          commitInfo.tags
        }

      infoTags.size shouldBe 1
      infoTags.head.isDefined shouldBe true
      infoTags.head.get shouldBe SimpleHook.testOutput
    }

  it should "run a simple hook and save its outputs to CommitInfo during an optimization" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._

      val df = spark.sparkContext.range(0, 10).toDF("id")
      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "id")
        .save(tmpDir)

      val indexedTable = QbeastContext.indexedTableFactory.getIndexedTable(QTableID(tmpDir))
      indexedTable.optimize(
        1L,
        Map(s"$PRE_COMMIT_HOOKS_PREFIX.hook" -> classOf[SimpleHook].getCanonicalName))

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val conf = deltaLog.newDeltaHadoopConf()

      val infoTags = deltaLog.store
        .read(FileNames.deltaFile(deltaLog.logPath, snapshot.version), conf)
        .map(Action.fromJson)
        .collect { case commitInfo: CommitInfo => commitInfo.tags }

      infoTags.size shouldBe 1
      infoTags.head.isDefined shouldBe true
      infoTags.head.get shouldBe SimpleHook.testOutput
    }

}
