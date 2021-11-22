/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.{QDataType, QTableID, Revision, RevisionBuilder}
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.SparkToQTypesUtils
import io.qbeast.transform.Transformer
import org.apache.spark.sql.DataFrame

import scala.util.matching.Regex

/**
 * Spark implementation of RevisionBuilder
 */
object SparkRevisionBuilder extends RevisionBuilder[DataFrame] {

  val SpecExtractor: Regex = "((^<column_name>[^/]+)/(^<transformer>[^/]+))".r

  def getColumnQType(columnName: String, dataFrame: DataFrame): QDataType = {
    SparkToQTypesUtils.convertDataTypes(dataFrame.schema(columnName).dataType)
  }

  override def createNewRevision(
      qtableID: QTableID,
      data: DataFrame,
      options: Map[String, String]): Revision = {

    val qbeastOptions = QbeastOptions(options)
    val columnSpecs = qbeastOptions.columnsToIndex
    val desiredCubeSize = qbeastOptions.cubeSize

    val transformers = columnSpecs.map {
      case SpecExtractor(columnName, transformerType) =>
        Transformer(transformerType, columnName, getColumnQType(columnName, data))

      case columnName =>
        Transformer(columnName, getColumnQType(columnName, data))

    }.toVector

    Revision.firstRevision(qtableID, desiredCubeSize, transformers)
  }

}
