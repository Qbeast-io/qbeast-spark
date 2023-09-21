/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import io.qbeast.spark.internal.sources.QbeastBaseRelation
import io.qbeast.spark.table.IndexedTable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connector.write.{
  LogicalWriteInfo,
  SupportsOverwrite,
  SupportsTruncate,
  V1Write,
  WriteBuilder
}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{Filter, InsertableRelation}

import scala.collection.convert.ImplicitConversions.`map AsScala`

/**
 * WriteBuilder implementation for Qbeast Format
 * @param info the write information
 * @param properties the table properties
 * @param indexedTable the Indexed Table
 */
class QbeastWriteBuilder(
    info: LogicalWriteInfo,
    properties: Map[String, String],
    indexedTable: IndexedTable)
    extends WriteBuilder
    with SupportsOverwrite
    with SupportsTruncate {

  private var forceOverwrite = false

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    // TODO: User filters to select existing data to remove
    //  The remaining and the inserted data are then to be written
    forceOverwrite = true
    this
  }

  override def truncate(): WriteBuilder = {
    forceOverwrite = true
    this
  }

  /**
   * Build an InsertableRelation to be able to write the data in QbeastFormat
   * @return the InsertableRelation with the corresponding method
   */
  override def build(): V1Write = new V1Write {

    override def toInsertableRelation: InsertableRelation = {

      new InsertableRelation {
        def insert(data: DataFrame, overwrite: Boolean): Unit = {
          val session = data.sparkSession
          val append = if (forceOverwrite) false else !overwrite

          // Passing the options in the query plan plus the properties
          // because columnsToIndex needs to be included in the contract
          val writeOptions = info.options().toMap ++ properties
          indexedTable.save(data, writeOptions, append)

          // TODO: Push this to Apache Spark
          // Re-cache all cached plans(including this relation itself, if it's cached) that refer
          // to this data source relation. This is the behavior for InsertInto
          session.sharedState.cacheManager.recacheByPlan(
            session,
            LogicalRelation(
              QbeastBaseRelation.createRelation(session.sqlContext, indexedTable, writeOptions)))
        }
      }
    }

  }

}
