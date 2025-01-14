package io.qbeast.core.model

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.scalatest.funsuite.AnyFunSuite

class QbeastOptionsTest extends AnyFunSuite {

  test("QbeastOptions should correctly parse options") {
    val optionsMap = Map(
      "columnsToIndex" -> "column1,column2",
      "cubeSize" -> "1000",
      "tableFormat" -> "delta",
      "columnStats" -> "{\"column1_min\":0,\"column1_max\":100}")

    val qbeastOptions = QbeastOptions(CaseInsensitiveMap(optionsMap))

    assert(qbeastOptions.columnsToIndex == Seq("column1", "column2"))
    assert(qbeastOptions.cubeSize == 1000)
    assert(qbeastOptions.tableFormat == "delta")
    assert(qbeastOptions.columnStats.contains("{\"column1_min\":0,\"column1_max\":100}"))
  }

  test("QbeastOptions should throw an exception for unsupported table formats") {
    val optionsMap = Map(
      "columnsToIndex" -> "column1,column2",
      "cubeSize" -> "1000",
      "tableFormat" -> "unsupportedFormat")

    val exception = intercept[Exception] {
      QbeastOptions(CaseInsensitiveMap(optionsMap))
    }

    assert(exception.getMessage.contains("Unsupported table format"))
  }

  test("QbeastOptions should handle hooks correctly") {

    val optionsMap = Map(
      "columnsToIndex" -> "column1,column2",
      "cubeSize" -> "1000",
      "tableFormat" -> "delta",
      "qbeastPreCommitHook.hook1" -> "com.example.Hook1",
      "qbeastPreCommitHook.hook1.arg" -> "arg1")

    val qbeastOptions = QbeastOptions(CaseInsensitiveMap(optionsMap))

    assert(qbeastOptions.hookInfo.nonEmpty)
    assert(qbeastOptions.hookInfo.head == HookInfo("hook1", "com.example.Hook1", Some("arg1")))
  }

  test("QbeastOptions.toMap should return a case-insensitive map with all options") {
    val optionsMap =
      Map("columnsToIndex" -> "column1,column2", "cubeSize" -> "1000", "tableFormat" -> "delta")

    val qbeastOptions = QbeastOptions(CaseInsensitiveMap(optionsMap))
    val resultMap = qbeastOptions.toMap

    assert(resultMap("columnsToIndex") == "column1,column2")
    assert(resultMap("cubeSize") == "1000")
    assert(resultMap("tableFormat") == "delta")
  }

  test("QbeastOptions.empty should create default options") {
    val emptyOptions = QbeastOptions.empty

    assert(emptyOptions.columnsToIndex.isEmpty)
    assert(emptyOptions.cubeSize == 0)
    assert(emptyOptions.tableFormat == "")
    assert(emptyOptions.columnStats.isEmpty)
    assert(emptyOptions.hookInfo.isEmpty)
    assert(emptyOptions.extraOptions.isEmpty)
  }

}
