/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.files

import io.qbeast.spark.model.Revision
import io.qbeast.spark.sql.qbeast
import io.qbeast.spark.sql.utils.QbeastExpressionUtils.{extractDataFilters, extractWeightRange}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}

/**
 * FileIndex to prune files
 *
 * @param index the Tahoe log file index
 */
case class OTreeIndex(index: TahoeLogFileIndex)
    extends TahoeFileIndex(index.spark, index.deltaLog, index.path) {

  /**
   * Snapshot to analyze
   * @return the snapshot
   */
  protected def snapshot: Snapshot = index.getSnapshot

  /**
   * QbeastSnapshot to analyze
   *
   * @return the qbeast snapshot
   */
  private def qbeastSnapshot = qbeast.QbeastSnapshot(snapshot)

  override def matchingFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[AddFile] = {

    val filters = partitionFilters ++ dataFilters
    val qbeastDataFilters =
      extractDataFilters(filters, qbeastSnapshot.lastRevision, SparkSession.active)
    val (minWeight, maxWeight) = extractWeightRange(qbeastDataFilters)

    // For each of the available revisions, sample the files required
    // through the revision data collected from the Delta Snapshot
    qbeastSnapshot.revisions
      .map { revision: Revision =>
        qbeastSnapshot
          .getRevisionData(revision.id)
          .sample(minWeight, maxWeight, qbeastDataFilters)

      }
      .reduce(_ ++ _)
  }

  override def inputFiles: Array[String] = {
    index.inputFiles
  }

  override def refresh(): Unit = index.refresh()

  override def sizeInBytes: Long = index.sizeInBytes
}
