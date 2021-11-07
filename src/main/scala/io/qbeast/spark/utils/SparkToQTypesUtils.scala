/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import io.qbeast.model.QTableID
import io.qbeast.{model => qmodel}
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.types._

object SparkToQTypesUtils {

  def loadFromParameters(parameters: Map[String, String]): QTableID = {
    new QTableID(
      parameters.getOrElse(
        "path", {
          throw AnalysisExceptionFactory.create("'path' is not specified")
        }))
  }

  def convertDataTypes(sparkType: DataType): qmodel.QDataType = sparkType match {
    case _: DoubleType => qmodel.DoubleDataType
    case _: IntegerType => qmodel.IntegerDataType
    case _: FloatType => qmodel.FloatDataType
    case _: LongType => qmodel.LongDataType
    case _ => throw new RuntimeException("Missing type")
    // TODO add more types
  }

}
