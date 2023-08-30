/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

import io.qbeast.core.{model => qmodel}
import org.apache.spark.sql.types._

object SparkToQTypesUtils {

  def convertDataTypes(sparkType: DataType): io.qbeast.core.model.QDataType = sparkType match {
    case _: DoubleType => qmodel.DoubleDataType
    case _: IntegerType => qmodel.IntegerDataType
    case _: FloatType => qmodel.FloatDataType
    case _: LongType => qmodel.LongDataType
    case _: StringType => qmodel.StringDataType
    case _: DecimalType => qmodel.DecimalDataType
    case _: TimestampType => qmodel.TimestampDataType
    case _: DateType => qmodel.DateDataType
    case _: TimestampNTZType => qmodel.TimestampNTZType
    case _ => throw new RuntimeException(s"${sparkType.typeName} is not supported yet")
    // TODO add more types
  }

}
