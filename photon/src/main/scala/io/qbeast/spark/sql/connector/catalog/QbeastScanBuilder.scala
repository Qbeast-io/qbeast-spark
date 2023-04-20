package io.qbeast.spark.sql.connector.catalog

import io.qbeast.spark.sql.execution.SampleOperator
import io.qbeast.spark.sql.execution.datasources.OTreePhotonIndex
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisExceptionFactory, SparkSession}
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownTableSample}
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * Creates a Scan for Qbeast Datasource with the corresponding pushdown operators
 * @param sparkSession the active Spark Session
 * @param schema the schema of the table
 * @param oTreePhotonIndex the index to prune files
 * @param options the options to read
 */

class QbeastScanBuilder(
    sparkSession: SparkSession,
    schema: StructType,
    dataSchema: StructType,
    oTreePhotonIndex: OTreePhotonIndex,
    options: CaseInsensitiveStringMap)
    extends ParquetScanBuilder(sparkSession, oTreePhotonIndex, schema, dataSchema, options)
    with SupportsPushDownTableSample
    with Logging {

  override def build(): Scan = {
    // We filter the files prior to initialise the ParquetScan
    // Due to similar issue https://github.com/microsoft/hyperspace/issues/302
    // where FileStatus is not compatible with internal Databricks SerializableFileStatus
    // TODO having two indexes is redundant,
    //  the main objective would be to filter directly with PhotonQueryManager
    val userSpecifiedSchema = oTreePhotonIndex.userSpecifiedSchema
    val rootPathsToRead =
      oTreePhotonIndex.listFiles(partitionFilters, dataFilters).flatMap(_.files.map(_.getPath))

    val inMemoryFileIndex =
      new InMemoryFileIndex(
        sparkSession,
        rootPathsToRead,
        options.asScala.toMap,
        userSpecifiedSchema)

    // Copy the new file index into scan builder
    // and call the superseed build scan
    val scanBuilder = super.copy(fileIndex = inMemoryFileIndex)
    scanBuilder.build()

  }

  /*
   * Pushes down table sample by deleting the operation from the SparkPlan
   * and updating pushdownSample var
   */
  override def pushTableSample(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long): Boolean = {
    if (upperBound < lowerBound) {
      throw AnalysisExceptionFactory.create(
        s"Sample lower bound $lowerBound is bigger than upper bound $upperBound")
    }
    // Pushdown sample to the OTreePhotonIndex
    oTreePhotonIndex.pushdownSample(SampleOperator(lowerBound, upperBound, withReplacement, seed))
    logInfo(
      s"QBEAST PUSHING DOWN SAMPLE lowerBound: $lowerBound," +
        s" upperBound: $upperBound, " +
        s"withReplacement: $withReplacement, " +
        s"seed: $seed")
    true
  }

}
