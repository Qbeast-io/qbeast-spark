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
package io.qbeast.core.model

import io.qbeast.core.model.PreCommitHook.getHookArgName
import io.qbeast.core.model.PreCommitHook.PRE_COMMIT_HOOKS_PREFIX
import io.qbeast.core.model.QbeastOptions.COLUMNS_TO_INDEX
import io.qbeast.core.model.QbeastOptions.COLUMN_STATS
import io.qbeast.core.model.QbeastOptions.CUBE_SIZE
import io.qbeast.core.model.QbeastOptions.TABLE_FORMAT
import io.qbeast.spark.index.ColumnsToIndex
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.qbeast.config.DEFAULT_TABLE_FORMAT
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.AnalysisExceptionFactory

import java.util.Locale
import scala.collection.mutable
import scala.util.matching.Regex

/**
 * QbeastOptions is a case class that encapsulates various options for Qbeast.
 *
 * @param columnsToIndex
 *   A sequence of column names to index.
 * @param cubeSize
 *   The number of desired elements per cube.
 * @param columnStats
 *   Optional DataFrame containing statistics for the indexing columns.
 * @param hookInfo
 *   A sequence of HookInfo objects representing the hooks to be executed.
 * @param extraOptions
 *   A map containing extra write options.
 */
case class QbeastOptions(
    columnsToIndex: Seq[String],
    cubeSize: Int,
    tableFormat: String,
    columnStats: Option[String] = None,
    hookInfo: Seq[HookInfo] = Nil,
    extraOptions: Map[String, String] = Map.empty[String, String]) {

  def writeProperties: Map[String, String] = {
    val properties = Map.newBuilder[String, String]
    properties += COLUMNS_TO_INDEX -> columnsToIndex.mkString(",")
    properties += CUBE_SIZE -> cubeSize.toString
    properties += TABLE_FORMAT -> tableFormat
    properties += COLUMN_STATS -> columnStats.getOrElse("")
    properties.result()
  }

  /**
   * Returns a sequence of ColumnToIndex objects representing the columns to index already parsed
   * @return
   */
  def columnsToIndexParsed: Seq[ColumnToIndex] = columnsToIndex.map(ColumnToIndex(_))

  def toMap: CaseInsensitiveMap[String] = {
    val options = Map.newBuilder[String, String]
    options += COLUMNS_TO_INDEX -> columnsToIndex.mkString(",")
    options += CUBE_SIZE -> cubeSize.toString
    options += TABLE_FORMAT -> tableFormat
    if (columnStats.nonEmpty) {
      options += COLUMN_STATS -> columnStats.get
    }
    if (hookInfo.nonEmpty) {
      hookInfo.foreach { options ++= _.toMap }
    }
    CaseInsensitiveMap(options.result() ++ extraOptions)
  }

}

/**
 * Options available when trying to write in qbeast format
 */

object QbeastOptions {
  val PATH: String = "path"
  val COLUMNS_TO_INDEX: String = "columnsToIndex"
  val CUBE_SIZE: String = "cubeSize"
  val TABLE_FORMAT: String = "tableFormat"
  val COLUMN_STATS: String = "columnStats"

  // All the write option keys
  val qbeastOptionKeys: Set[String] =
    Set(PATH, COLUMNS_TO_INDEX, CUBE_SIZE, TABLE_FORMAT, COLUMN_STATS)

  /**
   * Gets the columns to index from the options
   * @param options
   *   the options passed on the dataframe
   * @return
   */
  private def getColumnsToIndex(options: Map[String, String]): Seq[String] = {
    val encodedColumnsToIndex = options.getOrElse(
      COLUMNS_TO_INDEX, {
        throw AnalysisExceptionFactory.create(
          "you must specify the columns to index in a comma separated way" +
            " as .option(columnsToIndex, ...)")
      })
    ColumnsToIndex.decode(encodedColumnsToIndex)
  }

  /**
   * Gets the desired cube size from the options
   * @param options
   *   the options passed on the dataframe
   * @return
   */
  private def getDesiredCubeSize(options: Map[String, String]): Int = {
    options.get(CUBE_SIZE) match {
      case Some(value) => value.toInt
      case None => DEFAULT_CUBE_SIZE
    }
  }

  private def getTableFormat(options: Map[String, String]): String =
    options.get(TABLE_FORMAT) match {
      case Some(value) => value
      case None => DEFAULT_TABLE_FORMAT
    }

  /**
   * Get the column stats from the options. This provided column stats should be in a JSON
   * formatted string with the following schema {"columnName_min":value, "columnName_max":value,
   * "columnName_quantiles": [...], ...}
   * @param options
   *   the options passed on the dataframe
   * @return
   */
  private def getColumnStats(options: Map[String, String]): Option[String] =
    options.get(COLUMN_STATS)

  /**
   * This function is used to extract the information about hooks from the provided options. A
   * hook in this context is a piece of code that is executed before a commit.
   *
   * @param options
   *   A Map[String, String] that represents the options passed to the function. The options map
   *   should contain keys that match the pattern "${PRE_COMMIT_HOOKS_PREFIX.toLowerCase}.(\\w+)",
   *   where "\\w+" is the name of the hook. The corresponding value should be the full class name
   *   of the hook. Optionally, there can be another key for each hook with the pattern
   *   "${PRE_COMMIT_HOOKS_PREFIX.toLowerCase}.(\\w+).arg", where "\\w+" is the name of the hook.
   *   The corresponding value should be the argument for the hook.
   *
   * @return
   *   A Seq[HookInfo] that contains the information about each hook. Each HookInfo object
   *   contains the name of the hook, the full class name of the hook, and optionally the argument
   *   for the hook.
   */
  private def getHookInfo(options: Map[String, String]): Seq[HookInfo] = {
    val hookNamePattern: Regex = s"${PRE_COMMIT_HOOKS_PREFIX.toLowerCase}.(\\w+)".r
    options
      .map {
        case (hookNamePattern(hookName), clsFullName) =>
          val hookArgName = getHookArgName(hookName)
          Some(HookInfo(hookName, clsFullName, options.get(hookArgName)))
        case _ => None
      }
      .collect { case Some(hookInfo) => hookInfo }
      .toSeq
  }

  /**
   * Create QbeastOptions object from options map
   * @param options
   *   the options map
   * @return
   *   the QbeastOptions
   */
  def apply(options: CaseInsensitiveMap[String]): QbeastOptions = {
    val columnsToIndex = getColumnsToIndex(options)
    val desiredCubeSize = getDesiredCubeSize(options)
    val tableFormat = getTableFormat(options)
    val columnStats = getColumnStats(options)
    val hookInfo = getHookInfo(options)
    // Filter non-qbeast-related options
    val extraOptions =
      options.originalMap.filterKeys(key =>
        !qbeastOptionKeys.contains(key) && !key.startsWith(PRE_COMMIT_HOOKS_PREFIX))
    QbeastOptions(
      columnsToIndex,
      desiredCubeSize,
      tableFormat,
      columnStats,
      hookInfo,
      extraOptions)
  }

  /**
   * Create QbeastOptions object from options map
   * @param options
   *   the options map
   * @return
   *   the QbeastOptions
   */
  def apply(options: Map[String, String]): QbeastOptions = apply(CaseInsensitiveMap(options))

  /**
   * Create QbeastOptions object from options map for optimization
   * @param options
   *   the options map
   * @return
   */
  def apply(options: Map[String, String], revision: Revision): QbeastOptions = {
    val updatedOptions = mutable.Map[String, String](options.toSeq: _*)
    updatedOptions += (CUBE_SIZE -> revision.desiredCubeSize.toString)
    updatedOptions += (COLUMNS_TO_INDEX -> revision.columnTransformers
      .map(_.columnName)
      .mkString(","))
    updatedOptions += (TABLE_FORMAT -> DEFAULT_TABLE_FORMAT)
    apply(updatedOptions.toMap)
  }

  /**
   * The empty options to be used as a placeholder.
   */
  def empty: QbeastOptions = QbeastOptions(Seq.empty, DEFAULT_CUBE_SIZE, DEFAULT_TABLE_FORMAT)

  def loadTableIDFromParameters(parameters: Map[String, String]): QTableID = {
    new QTableID(
      parameters.getOrElse(
        PATH, {
          throw AnalysisExceptionFactory.create("'path' is not specified")
        }))
  }

  def checkQbeastProperties(parameters: Map[String, String]): Unit = {
    require(
      parameters.contains(COLUMNS_TO_INDEX) || parameters.contains(
        COLUMNS_TO_INDEX.toLowerCase(Locale.ROOT)),
      throw AnalysisExceptionFactory.create("'columnsToIndex is not specified"))
  }

}
