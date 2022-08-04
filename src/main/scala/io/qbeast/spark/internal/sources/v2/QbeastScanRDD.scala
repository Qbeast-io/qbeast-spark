package io.qbeast.spark.internal.sources.v2

import io.qbeast.context.QbeastContext._
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import io.qbeast.spark.table.IndexedTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.connector.read.V1Scan
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * Extends Scan Builder for V1 DataSource
 * It allows Spark to read from a Qbeast Formatted table
 * TODO include here the logic to get rid of the QbeastHash while reading the records
 * @param indexedTable the IndexedTable
 */
class QbeastScanRDD(indexedTable: IndexedTable) extends V1Scan {

  private lazy val qbeastBaseRelation =
    QbeastBaseRelation.forQbeastTable(indexedTable).asInstanceOf[HadoopFsRelation]

  override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T = {

    // TODO add PrunedFilteredScan as an extension and implement the methods
    new BaseRelation with TableScan {
      override def sqlContext: SQLContext = context

      override def schema: StructType = qbeastBaseRelation.schema

      override def buildScan(): RDD[Row] = {
        val schema = qbeastBaseRelation.schema

        // Ugly hack to convert the schema fields into Attributes
        val output = LogicalRelation
          .apply(qbeastBaseRelation)
          .resolve(schema, context.sparkSession.sessionState.analyzer.resolver)

        // TODO check the parameters passed to this method
        FileSourceScanExec(
          relation = qbeastBaseRelation,
          output = output,
          requiredSchema = schema,
          partitionFilters = Seq.empty,
          optionalBucketSet = None,
          optionalNumCoalescedBuckets = None,
          dataFilters = Seq.empty,
          tableIdentifier = None)
          .executeColumnar()
          .mapPartitions { batches =>
            batches.flatMap { batch =>
              batch
                .rowIterator()
                .asScala
                .map(internal => {
                  val encoder = RowEncoder(schema).resolveAndBind()
                  encoder.createDeserializer().apply(internal)
                })
            }
          }
      }
    }.asInstanceOf[T]
  }

  override def readSchema(): StructType = metadataManager.loadCurrentSchema(indexedTable.tableID)

}
