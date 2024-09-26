package io.qbeast.spark

import io.qbeast.core.model.PreCommitHook
import io.qbeast.core.model.PreCommitHook.PRE_COMMIT_HOOKS_PREFIX
import io.qbeast.core.model.PreCommitHook.PreCommitHookOutput
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.QbeastFile
import io.qbeast.spark.context.QbeastContext
import org.apache.spark.sql.delta.actions.Action
import org.apache.spark.sql.delta.actions.CommitInfo
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.DeltaLog

private class SimpleHook(kv: String) extends PreCommitHook {
  override val name: String = "SimpleHook"

  override def run(args: Seq[QbeastFile]): PreCommitHookOutput = {
    kv.split(":") match {
      case Array(k, v) => Map(k -> v)
      case _ => Map.empty
    }
  }

}

class PreCommitHookIntegrationTest extends QbeastIntegrationTestSpec {

  "PreCommitHook" should "run simple hooks and save their outputs to CommitInfo during writes" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      import spark.implicits._

      val df = spark.sparkContext.range(0, 10).toDF("id")

      df.write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option(s"$PRE_COMMIT_HOOKS_PREFIX.hook_1", classOf[SimpleHook].getCanonicalName)
        .option(s"$PRE_COMMIT_HOOKS_PREFIX.hook_1.arg", "k1:v1")
        .option(s"$PRE_COMMIT_HOOKS_PREFIX.hook_2", classOf[SimpleHook].getCanonicalName)
        .option(s"$PRE_COMMIT_HOOKS_PREFIX.hook_2.arg", "k2:v2")
        .save(tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val conf = deltaLog.newDeltaHadoopConf()

      val infoTags = deltaLog.store
        .read(FileNames.deltaFile(deltaLog.logPath, snapshot.version), conf)
        .map(Action.fromJson)
        .collect { case commitInfo: CommitInfo => commitInfo.tags }

      infoTags shouldBe Some(Map("k1" -> "v1", "k2" -> "v2")) :: Nil
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
        Map(
          s"$PRE_COMMIT_HOOKS_PREFIX.hook_1" -> classOf[SimpleHook].getCanonicalName,
          s"$PRE_COMMIT_HOOKS_PREFIX.hook_1.arg" -> "k1:v1",
          s"$PRE_COMMIT_HOOKS_PREFIX.hook_2" -> classOf[SimpleHook].getCanonicalName,
          s"$PRE_COMMIT_HOOKS_PREFIX.hook_2.arg" -> "k2:v2"))

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val snapshot = deltaLog.update()
      val conf = deltaLog.newDeltaHadoopConf()

      val infoTags = deltaLog.store
        .read(FileNames.deltaFile(deltaLog.logPath, snapshot.version), conf)
        .map(Action.fromJson)
        .collect { case commitInfo: CommitInfo => commitInfo.tags }

      infoTags shouldBe Some(Map("k1" -> "v1", "k2" -> "v2")) :: Nil
    }

}
