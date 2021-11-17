/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.IISeq
import io.qbeast.model.{QTableID, Revision}
import io.qbeast.spark.delta.{DeltaQbeastSnapshot, OTreeIndex}
import io.qbeast.transform.Transformer
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

/**
 * Implementation of BaseRelation which wraps the HadoopFsRelation
 *
 * @param relation the wrapped instance
 */
case class QbeastBaseRelation(relation: BaseRelation, private val revision: Revision)
    extends BaseRelation {
  override def sqlContext: SQLContext = relation.sqlContext

  override def schema: StructType = relation.schema

  def columnTransformers: IISeq[Transformer] = revision.columnTransformers

}

/**
 * Companion object for QbeastBaseRelation
 */
object QbeastBaseRelation {

  /**
   * Creates HadoopFsRelation that wraps an OTreeIndex instance
   * @param spark SparkSession
   * @param fileIndex the file index to access
   * @param schema the schema of the data
   * @param options the options to write
   * @return the HadoopFsRelation
   */
  private def createHadoopFsRelation(
      spark: SparkSession,
      fileIndex: FileIndex,
      schema: StructType,
      options: Map[String, String]) = {
    HadoopFsRelation(
      fileIndex,
      partitionSchema = StructType(Seq.empty),
      dataSchema = schema,
      bucketSpec = None,
      new ParquetFileFormat,
      options)(spark)
  }

  /**
   * Creates a QbeastBaseRelation instance
   * @param spark SparkSession
   * @param tableID the identifier of the table
   * @return the QbeastBaseRelation
   */

  def forTableID(spark: SparkSession, tableID: QTableID): QbeastBaseRelation = {

    val deltaLog = DeltaLog.forTable(spark, tableID.id)
    val snapshot = deltaLog.snapshot
    val qbeastSnapshot = DeltaQbeastSnapshot(snapshot)
    val revision = qbeastSnapshot.loadLatestRevision

    val tahoeFileIndex =
      TahoeLogFileIndex(spark, deltaLog, deltaLog.dataPath, snapshot, Seq.empty, false)
    val fileIndex = OTreeIndex(tahoeFileIndex)
    val schema = snapshot.schema
    val formatOptions = snapshot.metadata.format.options

    val baseRelation = createHadoopFsRelation(spark, fileIndex, schema, formatOptions)
    QbeastBaseRelation(baseRelation, revision)
  }

}
