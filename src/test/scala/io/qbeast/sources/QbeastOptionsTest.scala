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
package io.qbeast.sources

import io.qbeast.core.model._
import io.qbeast.core.model.PreCommitHook.PRE_COMMIT_HOOKS_PREFIX
import io.qbeast.core.model.QbeastOptions.COLUMNS_TO_INDEX
import io.qbeast.core.model.QbeastOptions.COLUMN_STATS
import io.qbeast.core.model.QbeastOptions.CUBE_SIZE
import io.qbeast.core.model.QbeastOptions.TABLE_FORMAT
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.qbeast.config.DEFAULT_TABLE_FORMAT
import org.apache.spark.sql.AnalysisException

class QbeastOptionsTest extends QbeastIntegrationTestSpec {

  "QbeastOptions" should "initialize with only one columnsToIndex" in withSpark { _ =>
    val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "id"))
    options.columnsToIndex shouldBe Seq("id")
    options.cubeSize shouldBe DEFAULT_CUBE_SIZE
    options.tableFormat shouldBe DEFAULT_TABLE_FORMAT
  }

  it should "initialize with multiple columnsToIndex" in withSpark { _ =>
    val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "id,name"))
    options.columnsToIndex shouldBe Seq("id", "name")
    options.cubeSize shouldBe DEFAULT_CUBE_SIZE
    options.tableFormat shouldBe DEFAULT_TABLE_FORMAT
  }

  it should "throw an exception if columnsToIndex is not provided" in withSpark { _ =>
    assertThrows[AnalysisException] { QbeastOptions(Map(CUBE_SIZE -> "10")) }
  }

  it should "initialize with cubeSize" in withSpark { _ =>
    val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "id", CUBE_SIZE -> "10"))
    options.cubeSize shouldBe 10
    options.columnsToIndex shouldBe Seq("id")
    options.tableFormat shouldBe DEFAULT_TABLE_FORMAT
  }

  it should "initialize with tableFormat" in withSpark { _ =>
    val options = QbeastOptions(Map(COLUMNS_TO_INDEX -> "id", TABLE_FORMAT -> "someFormat"))
    options.tableFormat shouldBe "someFormat"
    options.columnsToIndex shouldBe Seq("id")
    options.cubeSize shouldBe DEFAULT_CUBE_SIZE
  }

  it should "initialize with columnStats" in withSpark { _ =>
    val options = QbeastOptions(
      Map(COLUMNS_TO_INDEX -> "id", COLUMN_STATS -> """{"col1_min":1",col1_max":10}"""))
    options.columnStats shouldBe Some("""{"col1_min":1",col1_max":10}""")
    options.columnsToIndex shouldBe Seq("id")
    options.cubeSize shouldBe DEFAULT_CUBE_SIZE
    options.tableFormat shouldBe DEFAULT_TABLE_FORMAT
  }

  it should "store additional options in extraOptions" in withSpark { _ =>
    val additionalOptions = Map("key_1" -> "value_1", "key_2" -> "value_2", "key_3" -> "value_3")
    val options =
      QbeastOptions(Map(COLUMNS_TO_INDEX -> "id") ++ additionalOptions)
    additionalOptions.foreach { case (key, value) => options.extraOptions(key) shouldBe value }
    options.columnsToIndex shouldBe Seq("id")
    options.cubeSize shouldBe DEFAULT_CUBE_SIZE
    options.tableFormat shouldBe DEFAULT_TABLE_FORMAT
    options.columnStats shouldBe None
  }

  it should "initialize hookInfo correctly" in withSpark { _ =>
    val options = QbeastOptions(
      Map(
        COLUMNS_TO_INDEX -> "id",
        s"$PRE_COMMIT_HOOKS_PREFIX.hook1" -> "HookClass1",
        s"$PRE_COMMIT_HOOKS_PREFIX.hook2" -> "HookClass2",
        s"$PRE_COMMIT_HOOKS_PREFIX.hook2.arg" -> "HookClass2Arg"))

    options.hookInfo shouldBe Seq(
      HookInfo("hook1", "HookClass1", None),
      HookInfo("hook2", "HookClass2", Some("HookClass2Arg")))
  }

  it should "return a map with all the input configurations" in withSpark { _ =>
    // Initial optionsMap
    val optionsMap = Map(
      QbeastOptions.COLUMNS_TO_INDEX -> "id",
      QbeastOptions.CUBE_SIZE -> "10",
      QbeastOptions.TABLE_FORMAT -> "parquet",
      QbeastOptions.COLUMN_STATS -> """{"col1_min":1",col1_max":10}""",
      s"$PRE_COMMIT_HOOKS_PREFIX.hook2" -> "HookClass2",
      s"$PRE_COMMIT_HOOKS_PREFIX.hook2.arg" -> "HookClass2Arg")
    // Qbeast Options
    val qbeastOptionsMap = QbeastOptions(optionsMap).toMap
    // toMap method testing
    optionsMap.keys.foreach(qbeastOptionsMap.contains(_) shouldBe true)
  }

  it should "be able to create an instance from map and Revision" in withSpark { _ =>
    val properties = Map(
      COLUMN_STATS -> """{"col1_min":1,"col1_max":10}""",
      s"$PRE_COMMIT_HOOKS_PREFIX.hook" -> "HookClass",
      s"$PRE_COMMIT_HOOKS_PREFIX.hook.arg" -> "HookClassArg")
    val revision =
      Revision(
        revisionID = 1L,
        timestamp = 1L,
        tableID = QTableID("t"),
        desiredCubeSize = 100,
        Vector(LinearTransformer("col_1", IntegerDataType)),
        Nil)
    val options = QbeastOptions(properties, revision)
    options.columnsToIndex shouldBe Seq("col_1")
    options.cubeSize shouldBe 100
    options.tableFormat shouldBe DEFAULT_TABLE_FORMAT
    options.columnStats shouldBe Some("""{"col1_min":1,"col1_max":10}""")
    options.hookInfo shouldBe Seq(HookInfo("hook", "HookClass", Some("HookClassArg")))
  }

  "checkQbeastProperties" should "throw exception when missing columnsToIndex" in {
    an[AnalysisException] shouldBe thrownBy(QbeastOptions.checkQbeastProperties(Map.empty))
  }

}
