package io.qbeast.spark.sql.connector.catalog

import io.qbeast.spark.sql.execution.datasources.{OTreePhotonIndex, QbeastPhotonSnapshot}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

/**
 * QbeastTable for Spark
 * @param name the name of the table
 * @param sparkSession the current spark session
 * @param options the options
 * @param path the path of the table
 * @param userSpecifiedSchema the user specified schema, if any
 */
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
    val oTreePhotonIndex =
      OTreePhotonIndex(sparkSession, snapshot, options.asScala.toMap, userSpecifiedSchema)
    new QbeastScanBuilder(sparkSession, schema(), oTreePhotonIndex, options)
  }

}
