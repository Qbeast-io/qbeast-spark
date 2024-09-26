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
    case _ => throw new RuntimeException(s"${sparkType.typeName} is not supported yet")
    // TODO add more types
  }

}
