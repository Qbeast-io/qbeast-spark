/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import io.qbeast.spark.internal.sources.QbeastBaseRelation
import io.qbeast.spark.table.IndexedTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.connector.read.V1Scan
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
    QbeastBaseRelation.forQbeastTable(indexedTable)

  override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T = {

    // TODO add PrunedFilteredScan as an extension and implement the methods
    new BaseRelation with TableScan {
      override def sqlContext: SQLContext = context

      override def schema: StructType = qbeastBaseRelation.schema

      override def buildScan(): RDD[Row] = {

        // We output the rdd from the DataFrame scan
        // This is a hack, and the implementation should add more Qbeast logic
        val df = context.sparkSession.baseRelationToDataFrame(qbeastBaseRelation)

        // Return the RDD
        df.rdd
      }

    }.asInstanceOf[T]

  }

  override def readSchema(): StructType = qbeastBaseRelation.schema

}
