package io.qbeast.spark.internal.sources.v2

import io.qbeast.context.QbeastContext.metadataManager
import io.qbeast.core.model.QTableID
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters._

class QbeastTableDatasource private[sources] (
    spark: SparkSession,
    path: Path,
    options: Map[String, String])
    extends Table
    with SupportsWrite {
  private val tableID = QTableID(path.toString)

  override def name(): String = tableID.id

  override def schema(): StructType = metadataManager.loadCurrentSchema(tableID)

  override def capabilities(): util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
    TableCapability.V1_BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA).asJava

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new QbeastWriteBuilder
  }

}
