package io.qbeast.spark.sql.connector.catalog

import io.qbeast.spark.sql.execution.datasources.QbeastPhotonSnapshot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

case class QbeastTable(
    name: String,
    sparkSession: SparkSession,
    options: Map[String, String],
    path: String,
    userSpecifiedSchema: Option[StructType] = None)
    extends Table
    with SupportsRead {

  lazy val snapshot: QbeastPhotonSnapshot = QbeastPhotonSnapshot(sparkSession, path)

  override def schema(): StructType = snapshot.schema

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new QbeastScanBuilder(sparkSession, schema(), snapshot, options)
  }

}
