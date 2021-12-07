/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import com.typesafe.config.ConfigFactory
import io.qbeast.spark.index.ColumnsToIndex
import org.apache.spark.sql.AnalysisExceptionFactory

/**
 * Container for Qbeast options.
 * @param columnsToIndex value of columnsToIndex option
 * @param cubeSize value of cubeSize option
 */
case class QbeastOptions(columnsToIndex: Seq[String], cubeSize: Int)

/**
 * Options available when trying to write in qbeast format
 */
object QbeastOptions {
  val COLUMNS_TO_INDEX = "columnsToIndex"
  val CUBE_SIZE = "cubeSize"

  private def getDefaultDesiredSize =
    ConfigFactory.load().getInt("qbeast.index.defaultCubeSize")

  private def getColumnsToIndex(options: Map[String, String]): Seq[String] = {
    val encodedColumnsToIndex = options.getOrElse(
      COLUMNS_TO_INDEX, {
        throw AnalysisExceptionFactory.create(
          "you must specify the columns to index in a comma separated way" +
            " as .option(columnsToIndex, ...)")
      })
    ColumnsToIndex.decode(encodedColumnsToIndex)
  }

  private def getDesiredCubeSize(options: Map[String, String]): Int = {
    options.get(CUBE_SIZE) match {
      case Some(value) => value.toInt
      case None => getDefaultDesiredSize
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
    QbeastOptions(columnsToIndex, desiredCubeSize)
  }

}
