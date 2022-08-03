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

class QbeastWriteBuilder(info: LogicalWriteInfo, indexedTable: IndexedTable)
    extends WriteBuilder
    with V1WriteBuilder
    with SupportsOverwrite {

  override def overwrite(filters: Array[Filter]): WriteBuilder = this

  override def buildForV1Write(): InsertableRelation = {

    new InsertableRelation {
      def insert(data: DataFrame, overwrite: Boolean): Unit = {
        indexedTable.save(data, info.options().asCaseSensitiveMap().toMap, append = !overwrite)
      }
    }
  }

}
