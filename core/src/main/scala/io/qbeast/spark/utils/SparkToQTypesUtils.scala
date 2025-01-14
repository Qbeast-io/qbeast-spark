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
package io.qbeast.spark.utils

import io.qbeast.core.model.DateDataType
import io.qbeast.core.model.DecimalDataType
import io.qbeast.core.model.DoubleDataType
import io.qbeast.core.model.FloatDataType
import io.qbeast.core.model.IntegerDataType
import io.qbeast.core.model.LongDataType
import io.qbeast.core.model.QDataType
import io.qbeast.core.model.StringDataType
import io.qbeast.core.model.TimestampDataType
import org.apache.spark.sql.types._

object SparkToQTypesUtils {

  def convertToQDataType(sparkDataType: DataType): QDataType = sparkDataType match {
    case _: DoubleType => DoubleDataType
    case _: IntegerType => IntegerDataType
    case _: FloatType => FloatDataType
    case _: LongType => LongDataType
    case _: StringType => StringDataType
    case _: DecimalType => DecimalDataType
    case _: TimestampType => TimestampDataType
    case _: DateType => DateDataType
    case _ => throw new RuntimeException(s"${sparkDataType.typeName} is not supported.")
    // TODO add more types
  }

  def convertToSparkDataType(qDataType: QDataType): DataType = qDataType match {
    case DoubleDataType => DoubleType
    case IntegerDataType => IntegerType
    case FloatDataType => FloatType
    case LongDataType => LongType
    case StringDataType => StringType
    case DecimalDataType => DecimalType.SYSTEM_DEFAULT
    case TimestampDataType => TimestampType
    case DateDataType => DateType
    case _ =>
      throw new RuntimeException(s"No corresponding spark type is found for ${qDataType.name}.")
    // TODO add more types
  }

}
