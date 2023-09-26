/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.types.StructType

object SparkCatalogV2Util {

  /**
   * Converts DS v2 columns to StructType, which encodes column comment and default value to
   * StructField metadata. This is mainly used to define the schema of v2 scan, w.r.t. the columns
   * of the v2 table.
   */
  def v2ColumnsToStructType(columns: Array[Column]): StructType = {
    CatalogV2Util.v2ColumnsToStructType(columns)
  }

  def structTypeToV2Columns(schema: StructType): Array[Column] = {
    CatalogV2Util.structTypeToV2Columns(schema)
  }

}
