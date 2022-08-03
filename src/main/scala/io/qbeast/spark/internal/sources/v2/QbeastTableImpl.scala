package io.qbeast.spark.internal.sources.v2

import io.qbeast.context.QbeastContext._
import io.qbeast.core.model.QTableID
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.connector.catalog.TableCapability._
import io.qbeast.spark.table.IndexedTableFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

import java.util
import scala.collection.JavaConverters._

class QbeastTableImpl private[sources] (
    path: Path,
    options: Map[String, String],
    schema: Option[StructType] = None,
    private val tableFactory: IndexedTableFactory)
    extends Table
    with SupportsWrite {

  private val spark = SparkSession.active

  private val pathString = path.toString

  private val tableId = QTableID(pathString)

  private val indexedTable = tableFactory.getIndexedTable(tableId)

  /**
   * Checks if the table exists on the catalog
   * @return true if exists
   */
  def isCatalogTable: Boolean = {
    // TODO Check if exists on the catalog
    // I don't think this is the better way to do so
    val tableName = pathString.split("/").last
    spark.catalog.tableExists(tableName)
  }

  override def name(): String = tableId.id

  override def schema(): StructType = {
    if (schema.isDefined) schema.get
    else metadataManager.loadCurrentSchema(tableId)
  }

  override def capabilities(): util.Set[TableCapability] =
    Set(ACCEPT_ANY_SCHEMA, BATCH_READ, V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE).asJava

  // Returns the write builder for the query in info
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new QbeastWriteBuilder(info, indexedTable)
  }

  def toBaseRelation: BaseRelation = {
    QbeastBaseRelation.forQbeastTable(indexedTable)
  }

  override def properties(): util.Map[String, String] = options.asJava

  // TODO extend with SupportRead
  /*
  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val fileIndex = OTreeIndex(SparkSession.active, path)
    val partitioningAwareFileIndex = PartitioningAwareFileIndex()
    new FileScanBuilder(spark, fileIndex, schema()) {
      override def build(): Scan = ???
    }
  }

   */

}
