/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import com.typesafe.config.ConfigFactory
import io.qbeast.model.{QDataType, QTableID, Revision, RevisionBuilder}
import io.qbeast.spark.index.ColumnsToIndex
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.SparkToQTypesUtils
import io.qbeast.transform.Transformer
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame}

import scala.util.matching.Regex

object SparkRevisionBuilder extends RevisionBuilder[DataFrame] {
  private val defaultDesiredSize = ConfigFactory.load().getInt("qbeast.index.size")
  val SpecExtractor: Regex = "((^<column_name>[^/]+)/(^<transformer>[^/]+))".r

  def getColumnQType(columnName: String, dataFrame: DataFrame): QDataType = {
    SparkToQTypesUtils.convertDataTypes(dataFrame.schema(columnName).dataType)
  }

  override def createNewRevision(
      qtableID: QTableID,
      data: DataFrame,
      options: Map[String, String]): Revision = {

    val columnSpecs = getColumnsToIndex(options)

    val transformers = columnSpecs.map {
      case SpecExtractor(columnName, transformerType) =>
        Transformer(transformerType, columnName, getColumnQType(columnName, data))

      case columnName =>
        Transformer("linear", columnName, getColumnQType(columnName, data))

    }.toVector

    checkColumnsToIndexMatchData(data, transformers)
    Revision.firstRevision(qtableID, defaultDesiredSize, transformers)
  }

  def checkColumnsToIndexMatchData(data: DataFrame, columnsToIndex: Seq[Transformer]): Unit = {
    val dataTypes = data.schema.fields.map(field => field.name -> field.dataType).toMap
    for (column <- columnsToIndex) {
      val dataType = dataTypes.getOrElse(
        column.columnName, {
          throw AnalysisExceptionFactory.create(s"Column '$column' is not found in data.")
        })
      // TODO update this to the new data types.
      if (!dataType.isInstanceOf[NumericType]) {
        throw AnalysisExceptionFactory.create(s"Column '$column' is not numeric.")
      }
    }
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

}
