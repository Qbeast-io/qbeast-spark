package io.qbeast.spark.sql.connector.catalog

import io.qbeast.spark.sql.execution.{QueryOperators, SampleOperator}
import io.qbeast.spark.sql.execution.datasources.{OTreePhotonIndex, QbeastPhotonSnapshot}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SparkSession, sources}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownAggregates,
  SupportsPushDownTableSample
}
import org.apache.spark.sql.execution.datasources.{AggregatePushDownUtils, SparkDataSourceUtils}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.internal.connector.SupportsPushDownCatalystFilters
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable

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
    snapshot: QbeastPhotonSnapshot,
    options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownTableSample
    with SupportsPushDownCatalystFilters
    with SupportsPushDownAggregates {

  private var pushdownSample: Option[SampleOperator] = None

  private var pushDownAggregates: Option[Aggregation] = None

  private var partitionFilters: Seq[Expression] = Seq.empty

  private var dataFilters: Seq[Expression] = Seq.empty

  private var pushedDataFilters: Array[Filter] = Array.empty

  private val partitionSchema = StructType(Seq.empty)

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
    if (pushDownAggregates.isEmpty) {
      finalSchema = schema
    }

    // Initializes QueryOperators, which would be analyzed on the OTreePhotonIndex
    val queryOperators =
      QueryOperators(pushDownAggregates, pushdownSample, dataFilters ++ partitionFilters)

    // Initializes OTreePhotonIndex to filter the files accordingly
    val fileIndex = OTreePhotonIndex(
      sparkSession,
      snapshot,
      options.asCaseSensitiveMap().asScala.toMap,
      queryOperators,
      Some(schema))

    ParquetScan(
      sparkSession,
      hadoopConf,
      fileIndex,
      finalSchema,
      finalSchema,
      StructType(Seq.empty),
      pushedDataFilters,
      options,
      pushDownAggregates,
      partitionFilters,
      dataFilters)

  }

  override def pushTableSample(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long): Boolean = {
    pushdownSample = Some(SampleOperator(lowerBound, upperBound, withReplacement, seed))
    // scalastyle:off
    println(
      s"QBEAST PUSHING DOWN SAMPLE lowerBound: $lowerBound," +
        s" upperBound: $upperBound, " +
        s"withReplacement: $withReplacement, " +
        s"seed: $seed")
    true
  }

  /*
   * Push down data filters to the file source, so the data filters can be evaluated there to
   * reduce the size of the data to be read. By default, data filters are not pushed down.
   * File source needs to implement this method to push down data filters.
   */
  protected def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = dataFilters

  // Based on FileScanBuilder
  override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
    val (partitionFilters, dataFilters) =
      SparkDataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, filters)
    this.partitionFilters = partitionFilters
    this.dataFilters = dataFilters
    val translatedFilters = mutable.ArrayBuffer.empty[sources.Filter]
    for (filterExpr <- dataFilters) {
      val translated = SparkDataSourceUtils.translateFilter(filterExpr, true)
      if (translated.nonEmpty) {
        translatedFilters += translated.get
      }
    }

    pushedDataFilters = pushDataFilters(translatedFilters.toArray)
    dataFilters
  }

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
        this.pushDownAggregates = Some(aggregation)
        true
      case _ => false
    }
  }

}
