package io.qbeast.spark.sql.connector.catalog

import io.qbeast.spark.sql.execution.SampleOperator
import io.qbeast.spark.sql.execution.datasources.{OTreePhotonIndex}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{
  Scan,
  SupportsPushDownAggregates,
  SupportsPushDownTableSample
}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.{
  AggregatePushDownUtils,
  InMemoryFileIndex,
  SparkDataSourceUtils
}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._

/**
 * Creates a Scan for Qbeast Datasource with the corresponding pushdown operators
 * @param sparkSession the active Spark Session
 * @param schema the schema of the table
 * @param snapshot the current snapshot of the table
 * @param options the options to read
 */

class QbeastScanBuilder(
    sparkSession: SparkSession,
    schema: StructType,
    fileIndex: OTreePhotonIndex,
    options: CaseInsensitiveStringMap)
    extends FileScanBuilder(sparkSession, fileIndex, schema)
    with SupportsPushDownTableSample
    with SupportsPushDownAggregates
    with Logging {
  private var pushDownAggregate: Option[Aggregation] = None

  private var finalSchema: StructType = StructType(Seq.empty)

  lazy val hadoopConf: Configuration = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  override def build(): Scan = {

    // the `finalSchema` is either pruned in pushAggregation (if aggregates are
    // pushed down), or pruned in readDataSchema() (in regular column pruning). These
    // two are mutual exclusive.
    if (pushDownAggregate.isEmpty) {
      finalSchema = schema
    }

    val filesToRead =
      fileIndex.listFiles(partitionFilters, dataFilters).flatMap(_.files.map(_.getPath))

    val inMemoryFileIndex =
      new InMemoryFileIndex(sparkSession, filesToRead, options.asScala.toMap, None)

    ParquetScan(
      sparkSession,
      hadoopConf,
      inMemoryFileIndex,
      finalSchema,
      finalSchema,
      StructType(Seq.empty),
      pushedDataFilters,
      options,
      pushDownAggregate)

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
    // Pushdown sample to the OTreePhotonIndex
    fileIndex.pushdownSample(SampleOperator(lowerBound, upperBound, withReplacement, seed))
    logInfo(
      s"QBEAST PUSHING DOWN SAMPLE lowerBound: $lowerBound," +
        s" upperBound: $upperBound, " +
        s"withReplacement: $withReplacement, " +
        s"seed: $seed")
    true
  }

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = dataFilters

  override def pushedFilters: Array[Predicate] =
    SparkDataSourceUtils.mapFiltersToV2(pushedDataFilters).toArray

  // Based on ParquetScanBuilder
  override def pushAggregation(aggregation: Aggregation): Boolean = {
    if (!sparkSession.sessionState.conf.parquetAggregatePushDown) {
      return false
    }

    AggregatePushDownUtils.getSchemaForPushedAggregation(
      aggregation,
      schema,
      Set.empty,
      dataFilters) match {

      case Some(schema) =>
        finalSchema = schema
        this.pushDownAggregate = Some(aggregation)
        true
      case _ => false
    }
  }

}
