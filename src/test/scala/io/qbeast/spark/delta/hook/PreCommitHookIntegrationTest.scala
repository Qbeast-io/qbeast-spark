package io.qbeast.spark.delta.hook

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

  "PreCommitHook" should "run a simple hook and save its outputs to CommitInfo" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._

      val df = spark.sparkContext.range(0, 10).toDF("id")
      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("qbeastPreCommitHooks.simpleHook", classOf[SimpleHook].getCanonicalName)
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val conf = deltaLog.newDeltaHadoopConf()

      val info = deltaLog.store
        .read(FileNames.deltaFile(deltaLog.logPath, snapshot.version), conf)
        .map(Action.fromJson)
        .collect { case commitInfo: CommitInfo => commitInfo.tags }

      info.size shouldBe 1
      info.head.isDefined shouldBe true
      info.head.get shouldBe SimpleHook.testOutput
    }

}
