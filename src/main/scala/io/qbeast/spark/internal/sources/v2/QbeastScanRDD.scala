/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import io.qbeast.context.QbeastContext._
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import io.qbeast.spark.table.IndexedTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.{RowEncoder}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.connector.read.V1Scan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

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

        val rootPath = indexedTable.tableID.id

        // Get the files to load from the Relation index
        val filesToLoad = qbeastBaseRelation.location.listFiles(Seq.empty, Seq.empty)
        // Map the paths of the files with the rootPath
        val pathsToLoad =
          filesToLoad.flatMap(_.files.map(f => {
            val path = f.getPath
            if (path.isAbsolute) path.toString else rootPath + "/" + path.toString
          }))

        // We output the plan to build scan information
        // This is a hack, and the Scan should be done
        // with more Qbeast logic
        val df = context.sparkSession.read.format("parquet").load(pathsToLoad: _*)
        val encoder = RowEncoder(schema).resolveAndBind()
        val deserializer = encoder.createDeserializer()

        df.queryExecution.executedPlan.execute().mapPartitions { batches =>
          batches.map(deserializer.apply)
        }
      }
    }.asInstanceOf[T]

  }

  override def readSchema(): StructType = metadataManager.loadCurrentSchema(indexedTable.tableID)

}
