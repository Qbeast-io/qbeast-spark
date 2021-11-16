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
 * Implementation of BaseRelation which wraps the
 * HadoopFsRelation
 *
 * @param baseRelation the wrapped instance
 */
case class QbeastBaseRelation(baseRelation: BaseRelation, private val revision: Revision)
    extends BaseRelation {
  override def sqlContext: SQLContext = baseRelation.sqlContext

  override def schema: StructType = baseRelation.schema

  def columnTransformers: IISeq[Transformer] = revision.columnTransformers

}

object QbeastBaseRelation {

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

  def forTableID(spark: SparkSession, qTableID: QTableID): QbeastBaseRelation = {

    val deltaLog = DeltaLog.forTable(spark, qTableID.id)
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
