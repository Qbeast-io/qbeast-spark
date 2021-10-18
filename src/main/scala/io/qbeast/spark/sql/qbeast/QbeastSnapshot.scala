/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.ColumnsToIndex
import io.qbeast.spark.model.Revision
import io.qbeast.spark.sql.utils.TagUtils._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{AnalysisExceptionFactory, Dataset, SparkSession}

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param snapshot the internal Delta Lakes log snapshot
 */
case class QbeastSnapshot(snapshot: Snapshot, desiredCubeSize: Int) {

  val indexId = s"qbeast.${snapshot.path.getParent.toUri.getPath}"

  def isInitial: Boolean = snapshot.version == -1

  val indexedCols: Seq[String] = {
    if (isInitial || snapshot.allFiles.isEmpty) Seq.empty
    else ColumnsToIndex.decode(snapshot.allFiles.head.tags(indexedColsTag))
  }

  val dimensionCount: Int = indexedCols.length

  private val spark = SparkSession.active
  import spark.implicits._

  /**
   * Looks up for a space revision with a certain timestamp
   * @param timestamp the timestamp of the revision
   * @return a SpaceRevision with the corresponding timestamp if any
   */
  def getRevisionSnapshotAt(timestamp: Long): RevisionSnapshot = {
    revisionSnapshots
      .find(_.revision.timestamp.equals(timestamp))
      .getOrElse(
        throw AnalysisExceptionFactory.create(s"No space revision available at $timestamp"))
  }

  /**
   * Returns available revisions ordered by timestamp
   * @return a Dataset of Revision
   */
  def allRevisions: Dataset[Revision] =
    snapshot.allFiles
      .select(s"tags.$spaceTag")
      .distinct
      .map(a => JsonUtils.fromJson[Revision](a.getString(0)))
      .orderBy(col("timestamp").desc)

  /**
   * Returns the space revision with the higher timestamp
   * @return the space revision
   */
  def lastSpaceRevision: Revision = {
    // Dataset spaceRevisions is ordered by timestamp
    allRevisions
      .first()

  }

  def existsRevision(timestamp: Long): Boolean = {
    revisionSnapshots.exists(_.revision.timestamp.equals(timestamp))
  }

  lazy val txnVersion = {
    snapshot.setTransactions.filter(_.appId.equals(indexId)) match {
      case Nil => -1
      case list => list.last.version
    }
  }

  lazy val revisionSnapshots: Seq[RevisionSnapshot] = {
    allRevisions
      .collect()
      .map(revision => {
        val revisionFiles = snapshot.allFiles.filter(_.tags(spaceTag).equals(revision.toString))
        RevisionSnapshot(revision, revisionFiles, txnVersion, snapshot.path.getParent)
      })
  }

  lazy val lastRevisionSnapshot: RevisionSnapshot = {
    getRevisionSnapshotAt(lastSpaceRevision.timestamp)
  }

  lazy val lastRevisionTimestamp: Long = lastSpaceRevision.timestamp

}
