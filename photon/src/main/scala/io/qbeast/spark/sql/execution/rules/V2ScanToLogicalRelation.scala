package io.qbeast.spark.sql.execution.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.{
  DataSourceV2Relation,
  DataSourceV2ScanRelation
}
import org.apache.spark.sql.execution.datasources.{
  FileSourceStrategy,
  HadoopFsRelation,
  LogicalRelation
}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import io.qbeast.spark.sql.connector.catalog.QbeastTable
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan

import scala.collection.JavaConverters._

class V2ScanToLogicalRelation(sparkSession: SparkSession)
    extends SparkStrategy
    with PredicateHelper {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    val newLogicalPlan = plan transformDown {
      case DataSourceV2ScanRelation(
            DataSourceV2Relation(table: QbeastTable, output, _, _, options),
            scan: ParquetScan,
            _,
            _) =>
        // get the file index from the scan,
        // which will have the paths to read from already filtered
        val fileIndex = scan.fileIndex
        val v1FileFormat = new ParquetFileFormat()

        val relation = HadoopFsRelation(
          fileIndex,
          fileIndex.partitionSchema,
          table.schema(),
          None,
          v1FileFormat,
          options.asScala.toMap)(sparkSession)
        LogicalRelation(relation, isStreaming = false).copy(output = output)

      case other =>
        other

    }

    FileSourceStrategy(newLogicalPlan)
  }

}
