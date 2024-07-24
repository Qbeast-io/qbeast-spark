package io.qbeast.core.model

import io.qbeast.spark.utils.SparkToQTypesUtils
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex

object ColumnToIndexUtils {

  val SpecExtractor: Regex = "([^:]+):([A-z]+)".r

  def getColumnQType(columnName: String, schema: StructType): QDataType = {
    SparkToQTypesUtils.convertDataTypes(schema(columnName).dataType)
  }

}
