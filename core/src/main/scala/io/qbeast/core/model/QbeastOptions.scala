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
import io.qbeast.core.model.QbeastOptions.CUBE_SIZE
import io.qbeast.core.model.QbeastOptions.STATS
import io.qbeast.core.model.QbeastOptions.TABLE_FORMAT
import io.qbeast.spark.index.ColumnsToIndex
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.qbeast.config.DEFAULT_TABLE_FORMAT
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.matching.Regex

/**
 * QbeastOptions is a case class that encapsulates various options for Qbeast.
 *
 * @param columnsToIndex
 *   A sequence of column names to index.
 * @param cubeSize
 *   The number of desired elements per cube.
 * @param stats
 *   Optional DataFrame containing statistics for the indexing columns.
 * @param txnAppId
 *   Optional transaction application ID.
 * @param txnVersion
 *   Optional transaction version.
 * @param userMetadata
 *   Optional user-provided metadata for each CommitInfo.
 * @param mergeSchema
 *   Optional flag indicating whether to merge the schema.
 * @param overwriteSchema
 *   Optional flag indicating whether to overwrite the schema.
 * @param hookInfo
 *   A sequence of HookInfo objects representing the hooks to be executed.
 * @param extraOptions
 *   A map containing extra write options.
 */
case class QbeastOptions(
    columnsToIndex: Seq[String],
    cubeSize: Int,
    tableFormat: String,
    statsString: Option[String],
    stats: Option[DataFrame],
    txnAppId: Option[String],
    txnVersion: Option[String],
    userMetadata: Option[String],
    mergeSchema: Option[String],
    overwriteSchema: Option[String],
    hookInfo: Seq[HookInfo] = Nil,
    extraOptions: Map[String, String] = Map.empty[String, String]) {

  def writeProperties: Map[String, String] = {
    val properties = Map.newBuilder[String, String]
    properties += COLUMNS_TO_INDEX -> columnsToIndex.mkString(",")
    properties += CUBE_SIZE -> cubeSize.toString
    properties += TABLE_FORMAT -> tableFormat
    properties += STATS -> statsString.getOrElse("")
    properties.result()
  }

  /**
   * Returns a sequence of ColumnToIndex objects representing the columns to index already parsed
   * @return
   */
  def columnsToIndexParsed: Seq[ColumnToIndex] = columnsToIndex.map(ColumnToIndex(_))

  def toMap: CaseInsensitiveMap[String] = {
    val options = Map.newBuilder[String, String]
    for (txnAppId <- txnAppId; txnVersion <- txnVersion) {
      options += "txnAppId" -> txnAppId
      options += "txnVersion" -> txnVersion
    }
    if (userMetadata.nonEmpty) {
      options += "userMetadata" -> userMetadata.get
    }
    if (mergeSchema.nonEmpty) {
      options += "mergeSchema" -> mergeSchema.get
    }
    if (overwriteSchema.nonEmpty) {
      options += "overwriteSchema" -> overwriteSchema.get
    }
    if (hookInfo.nonEmpty) {
      hookInfo.foreach { options ++= _.toMap }
    }

    options += COLUMNS_TO_INDEX -> columnsToIndex.mkString(",")
    options += CUBE_SIZE -> cubeSize.toString
    options += TABLE_FORMAT -> tableFormat
    options += STATS -> statsString.getOrElse("")

    CaseInsensitiveMap(options.result())
  }

}

/**
 * Options available when trying to write in qbeast format
 */

object QbeastOptions {
  val COLUMNS_TO_INDEX: String = "columnsToIndex"
  val CUBE_SIZE: String = "cubeSize"
  val TABLE_FORMAT: String = "tableFormat"
  val PATH: String = "path"
  val STATS: String = "columnStats"
  val TXN_APP_ID: String = "txnAppId"
  val TXN_VERSION: String = "txnVersion"
  val USER_METADATA: String = "userMetadata"
  val MERGE_SCHEMA: String = "mergeSchema"
  val OVERWRITE_SCHEMA: String = "overwriteSchema"

  // All the write option keys
  val qbeastOptionKeys = Set(
    COLUMNS_TO_INDEX,
    CUBE_SIZE,
    TABLE_FORMAT,
    PATH,
    STATS,
    TXN_APP_ID,
    TXN_VERSION,
    USER_METADATA,
    MERGE_SCHEMA,
    OVERWRITE_SCHEMA)

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
   * Get the column stats from the options This stats should be in a JSON formatted string with
   * the following schema {columnName_min:value, columnName_max:value, ...}
   * @param options
   *   the options passed on the dataframe
   * @return
   */
  private def getStats(options: Map[String, String]): Option[DataFrame] = {
    val spark = SparkSession.active

    options.get(STATS) match {
      case Some(value) =>
        import spark.implicits._
        val ds = Seq(value).toDS()
        val df = spark.read
          .option("inferTimestamp", "true")
          .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSSSSS'Z'")
          .json(ds)
        Some(df)
      case None => None
    }
  }

  private def getTxnAppId(options: Map[String, String]): Option[String] = options.get(TXN_APP_ID)

  private def getTxnVersion(options: Map[String, String]): Option[String] =
    options.get(TXN_VERSION)

  private def getUserMetadata(options: Map[String, String]): Option[String] =
    options.get(USER_METADATA)

  private def getMergeSchema(options: Map[String, String]): Option[String] =
    options.get(MERGE_SCHEMA)

  private def getOverwriteSchema(options: Map[String, String]): Option[String] =
    options.get(OVERWRITE_SCHEMA)

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
    val statsString = options.get(STATS)
    val stats = getStats(options)
    val txnAppId = getTxnAppId(options)
    val txnVersion = getTxnVersion(options)
    val userMetadata = getUserMetadata(options)
    val mergeSchema = getMergeSchema(options)
    val overwriteSchema = getOverwriteSchema(options)
    val hookInfo = getHookInfo(options)
    // Filter out the qbeast options, and leave: COLUMNSTOINDEX, CUBESIZE, AND STATS
    // Plus the user metadata, merge schema, and overwrite schema
    val caseSensitiveMap = options.originalMap
    val extraOptions =
      caseSensitiveMap.filterKeys(key =>
        !qbeastOptionKeys.contains(key) && !key.startsWith(PRE_COMMIT_HOOKS_PREFIX))

    QbeastOptions(
      columnsToIndex,
      desiredCubeSize,
      tableFormat,
      statsString,
      stats,
      txnAppId,
      txnVersion,
      userMetadata,
      mergeSchema,
      overwriteSchema,
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
  def optimizationOptions(options: Map[String, String]): QbeastOptions = {
    val caseInsensitiveMap = CaseInsensitiveMap(options)
    val userMetadata = getUserMetadata(caseInsensitiveMap)
    val hookInfo = getHookInfo(caseInsensitiveMap)
    QbeastOptions(
      Seq.empty,
      0,
      DEFAULT_TABLE_FORMAT,
      None,
      None,
      None,
      None,
      userMetadata,
      None,
      None,
      hookInfo)
  }

  /**
   * The empty options to be used as a placeholder.
   */
  lazy val empty: QbeastOptions =
    QbeastOptions(
      Seq.empty,
      DEFAULT_CUBE_SIZE,
      DEFAULT_TABLE_FORMAT,
      None,
      None,
      None,
      None,
      None,
      None,
      None)

  def loadTableIDFromParameters(parameters: Map[String, String]): QTableID = {
    new QTableID(
      parameters.getOrElse(
        PATH, {
          throw AnalysisExceptionFactory.create("'path' is not specified")
        }))
  }

  def checkQbeastProperties(parameters: Map[String, String]): Unit = {
    require(
      parameters.contains("columnsToIndex") || parameters.contains("columnstoindex"),
      throw AnalysisExceptionFactory.create("'columnsToIndex is not specified"))
  }

}
