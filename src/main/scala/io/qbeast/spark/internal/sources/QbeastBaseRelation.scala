/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.IISeq
import io.qbeast.core.model.{QTableID, Revision}
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.core.transform.Transformer
import org.apache.spark.sql.{SQLContext, DataFrame, AnalysisExceptionFactory}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.SparkSession
import io.qbeast.spark.delta.OTreeIndex
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.delta.files.TahoeLogFileIndex

import org.apache.spark.sql.delta.DeltaFileFormat
import io.qbeast.spark.table.IndexedTableFactory
import io.qbeast.spark.utils.SparkToQTypesUtils
import org.apache.spark.sql.execution.datasources.FileFormat

import org.apache.hadoop.fs.{Path}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec}
import org.apache.spark.sql.{
  AnalysisExceptionFactory,
  DataFrame,
  SQLContext,
  SaveMode,
  SparkSession
}

/**
 * Implementation of BaseRelation which wraps the Delta relation.
 *
 * @param relation the wrapped instance
 * @param revision the revision to use
 */
case class QbeastBaseRelation(relation: BaseRelation, private val revision: Revision)
    extends BaseRelation
    with InsertableRelation
    with DeltaFileFormat {
  override def sqlContext: SQLContext = relation.sqlContext

  override def schema: StructType = relation.schema

  // override def insert(data: DataFrame, overwrite: Boolean): Unit = {
  //   asSelectQuery = Option(data)
  // }

  def columnTransformers: IISeq[Transformer] = revision.columnTransformers

}

/**
 * Companion object for QbeastBaseRelation
 */
object QbeastBaseRelation {

  /**
   * Creates a QbeastBaseRelation instance
   * @param tableID the identifier of the table
   * @return the QbeastBaseRelation
   */

  def forDeltaTable(tableID: QTableID): QbeastBaseRelation = {

    val log = SparkDeltaMetadataManager.loadDeltaQbeastLog(tableID)

    QbeastBaseRelation(log.createRelation, log.qbeastSnapshot.loadLatestRevision)

  }

}
