/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import io.qbeast.core.model.QTableID
import io.qbeast.spark.index.ColumnsToIndex
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame, SparkSession}

/**
 * Container for Qbeast options.
 * @param columnsToIndex value of columnsToIndex option
 * @param cubeSize value of cubeSize option
 */
case class QbeastOptions(columnsToIndex: Seq[String], cubeSize: Int, stats: DataFrame)

/**
 * Options available when trying to write in qbeast format
 */

object QbeastOptions {
  val COLUMNS_TO_INDEX = "columnsToIndex"
  val CUBE_SIZE = "cubeSize"
  val PATH = "path"
  val STATS = "stats"

  /**
   * Gets the columns to index from the options
   * @param options the options passed on the dataframe
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
   * @param options the options passed on the dataframe
   * @return
   */

  private def getDesiredCubeSize(options: Map[String, String]): Int = {
    options.get(CUBE_SIZE) match {
      case Some(value) => value.toInt
      case None => DEFAULT_CUBE_SIZE
    }
  }

  /**
   * Get the column stats from the options
   * This stats should be in a JSON formatted string
   * with the following schema
   * {min(a):value, max(a):value, min(b):value...}
   * @param options the options passed on the dataframe
   * @return
   */
  private def getStats(options: Map[String, String]): DataFrame = {
    val spark = SparkSession.active

    options.get(STATS) match {
      case Some(value) =>
        import spark.implicits._
        spark.read.json(Seq(value).toDS)
      case None => spark.emptyDataFrame
    }
  }

  /**
   * Create QbeastOptions object from options map
   * @param options the options map
   * @return the QbeastOptions
   */
  def apply(options: Map[String, String]): QbeastOptions = {
    val columnsToIndex = getColumnsToIndex(options)
    val desiredCubeSize = getDesiredCubeSize(options)
    val stats = getStats(options)
    QbeastOptions(columnsToIndex, desiredCubeSize, stats)
  }

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
