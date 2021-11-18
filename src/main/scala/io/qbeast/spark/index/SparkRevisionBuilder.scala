/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import com.typesafe.config.ConfigFactory
import io.qbeast.model.{QDataType, QTableID, Revision, RevisionBuilder}
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.SparkToQTypesUtils
import io.qbeast.transform.Transformer
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame}

import scala.util.matching.Regex

/**
 * Spark implementation of RevisionBuilder
 */
object SparkRevisionBuilder extends RevisionBuilder[DataFrame] {

  private def defaultDesiredSize =
    ConfigFactory.load().getInt("qbeast.index.defaultCubeSize")

  val SpecExtractor: Regex = "((^<column_name>[^/]+)/(^<transformer>[^/]+))".r

  def getColumnQType(columnName: String, dataFrame: DataFrame): QDataType = {
    SparkToQTypesUtils.convertDataTypes(dataFrame.schema(columnName).dataType)
  }

  override def createNewRevision(
      qtableID: QTableID,
      data: DataFrame,
      options: Map[String, String]): Revision = {

    val columnSpecs = getColumnsToIndex(options)
    val desiredCubeSize = getDesiredCubeSize(options)

    val transformers = columnSpecs.map {
      case SpecExtractor(columnName, transformerType) =>
        Transformer(transformerType, columnName, getColumnQType(columnName, data))

      case columnName =>
        Transformer(columnName, getColumnQType(columnName, data))

    }.toVector

    Revision.firstRevision(qtableID, desiredCubeSize, transformers)
  }

  private def getColumnsToIndex(parameters: Map[String, String]): Seq[String] = {
    val encodedColumnsToIndex = parameters.getOrElse(
      QbeastOptions.COLUMNS_TO_INDEX, {
        throw AnalysisExceptionFactory.create(
          "you must specify the columns to index in a comma separated way" +
            " as .option(columnsToIndex, ...)")
      })
    ColumnsToIndex.decode(encodedColumnsToIndex)
  }

  private def getDesiredCubeSize(parameters: Map[String, String]): Int = {
    parameters.get(QbeastOptions.CUBE_SIZE) match {
      case Some(value) => value.toInt
      case None => defaultDesiredSize
    }
  }

}
