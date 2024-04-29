package io.qbeast.spark.internal.sources

import io.qbeast.spark.delta.hook.HookInfo
import io.qbeast.spark.delta.hook.PreCommitHook.PRE_COMMIT_HOOKS_PREFIX
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.qbeast.config
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.AnalysisException

class QbeastOptionsTest extends QbeastIntegrationTestSpec {

  "QbeastOptions" should "be able to create a QbeastOptions instance only with columnsToIndex" in withSpark {
    _ =>
      val options = QbeastOptions(Map("columnsToIndex" -> "id"))
      options.columnsToIndex shouldBe Seq("id")
  }

  it should "throw an exception if columnsToIndex is not provided" in withSpark { _ =>
    assertThrows[AnalysisException] {
      QbeastOptions(Map("cubeSize" -> "10"))
    }
  }

  it should "be able to create a QbeastOptions instance with multiple columnsToIndex" in withSpark {
    _ =>
      val options = QbeastOptions(Map("columnsToIndex" -> "id,name"))
      options.columnsToIndex shouldBe Seq("id", "name")
  }

  it should "be able to create a QbeastOptions instance with cube size" in withSpark { _ =>
    val options = QbeastOptions(Map("columnsToIndex" -> "id", "cubeSize" -> "10"))
    options.cubeSize shouldBe 10
  }

  it should "initialize with default cube size" in withSpark { _ =>
    val options = QbeastOptions(Map("columnsToIndex" -> "id"))
    options.cubeSize shouldBe config.DEFAULT_CUBE_SIZE
  }

  it should "initialize txnAppId and txnVersion" in withSpark { _ =>
    val options =
      QbeastOptions(Map("columnsToIndex" -> "id", "txnAppId" -> "app", "txnVersion" -> "version"))
    options.txnAppId shouldBe Some("app")
    options.txnVersion shouldBe Some("version")
  }

  it should "initialize userMetadata" in withSpark { _ =>
    val options = QbeastOptions(Map("columnsToIndex" -> "id", "userMetadata" -> "metadata"))
    options.userMetadata shouldBe Some("metadata")
  }

  it should "initialize mergeSchema" in withSpark { _ =>
    val options = QbeastOptions(Map("columnsToIndex" -> "id", "mergeSchema" -> "true"))
    options.mergeSchema shouldBe Some("true")
  }

  it should "initialize overwriteSchema" in withSpark { _ =>
    val options = QbeastOptions(Map("columnsToIndex" -> "id", "overwriteSchema" -> "true"))
    options.overwriteSchema shouldBe Some("true")
  }

  it should "throw exception if checking missing columnsToIndex" in {
    an[AnalysisException] shouldBe thrownBy(QbeastOptions.checkQbeastProperties(Map.empty))
  }

  it should "return a map with all the configurations including Delta specifics" in withSpark {
    _ =>
      // Initial optionsMap
      val optionsMap = Map(
        QbeastOptions.COLUMNS_TO_INDEX -> "id",
        QbeastOptions.CUBE_SIZE -> "10",
        QbeastOptions.TXN_APP_ID -> "app",
        QbeastOptions.TXN_VERSION -> "1",
        DeltaOptions.USER_METADATA_OPTION -> "metadata",
        DeltaOptions.MERGE_SCHEMA_OPTION -> "true",
        DeltaOptions.OVERWRITE_SCHEMA_OPTION -> "true",
        s"$PRE_COMMIT_HOOKS_PREFIX.hook2" -> "HookClass2",
        s"$PRE_COMMIT_HOOKS_PREFIX.hook2.arg" -> "HookClass2Arg")

      // Qbeast Options
      val options = QbeastOptions(optionsMap)
      // toMap method testing
      options.toMap shouldBe optionsMap
  }

  it should "initialize hookInfo correctly" in withSpark { _ =>
    val options = QbeastOptions(
      Map(
        "columnsToIndex" -> "id",
        s"$PRE_COMMIT_HOOKS_PREFIX.hook1" -> "HookClass1",
        s"$PRE_COMMIT_HOOKS_PREFIX.hook2" -> "HookClass2",
        s"$PRE_COMMIT_HOOKS_PREFIX.hook2.arg" -> "HookClass2Arg"))

    options.hookInfo shouldBe Seq(
      HookInfo("hook1", "HookClass1", None),
      HookInfo("hook2", "HookClass2", Some("HookClass2Arg")))
  }

  "optimizationOptions" should "return a QbeastOption with proper settings" in {
    val options = Map(
      DeltaOptions.USER_METADATA_OPTION -> "metadata",
      s"$PRE_COMMIT_HOOKS_PREFIX.hook" -> "HookClass",
      s"$PRE_COMMIT_HOOKS_PREFIX.hook.arg" -> "HookClassArg")
    val qo = QbeastOptions.optimizationOptions(options)
    qo.userMetadata shouldBe Some("metadata")
    qo.hookInfo shouldBe Seq(HookInfo("hook", "HookClass", Some("HookClassArg")))
  }

}
