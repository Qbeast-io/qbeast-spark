/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import io.qbeast.spark.table.IndexedTable
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.connector.write.{
  LogicalWriteInfo,
  SupportsOverwrite,
  V1WriteBuilder,
  WriteBuilder
}
import org.apache.spark.sql.sources.{Filter, InsertableRelation}

import scala.collection.convert.ImplicitConversions.`map AsScala`

class QbeastWriteBuilder(
    info: LogicalWriteInfo,
    properties: Map[String, String],
    indexedTable: IndexedTable)
    extends WriteBuilder
    with V1WriteBuilder
    with SupportsOverwrite {

  override def overwrite(filters: Array[Filter]): WriteBuilder = this

  /**
   * Build an InsertableRelation to be able to write the data in QbeastFormat
   * @return the InsertableRelation with the corresponding method
   */
  override def buildForV1Write(): InsertableRelation = {

    new InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        // Passing the options in the query plan plus the properties
        // because columnsToIndex needs to be included in the contract
        val writeOptions = info.options().toMap ++ properties
        indexedTable.save(data, writeOptions, append = !overwrite)
      }
    }
  }

}
