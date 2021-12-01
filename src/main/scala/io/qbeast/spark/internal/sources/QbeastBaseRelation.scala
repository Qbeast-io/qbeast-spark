/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.IISeq
import io.qbeast.core.model.{QTableID, Revision}
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.core.transform.Transformer
import org.apache.spark.sql.SQLContext
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
   * Creates a QbeastBaseRelation instance
   * @param tableID the identifier of the table
   * @return the QbeastBaseRelation
   */

  def forDeltaTable(tableID: QTableID): QbeastBaseRelation = {
    val log = SparkDeltaMetadataManager.loadDeltaQbeastLog(tableID)
    QbeastBaseRelation(log.createRelation, log.qbeastSnapshot.loadLatestRevision)
  }

}
