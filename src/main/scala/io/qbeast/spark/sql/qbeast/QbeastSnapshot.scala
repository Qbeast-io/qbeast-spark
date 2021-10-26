/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.ColumnsToIndex
import io.qbeast.spark.model.{Revision, RevisionID}
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
   * Looks up for a revision with a certain identifier
   * @param revisionID the ID of the revision
   * @return RevisionData for the corresponding identifier
   */
  def getRevisionData(revisionID: RevisionID): RevisionData = {
    revisionsData
      .find(_.revision.timestamp == revisionID)
      .getOrElse(
        throw AnalysisExceptionFactory.create(s"No space revision available with $revisionID"))
  }

  /**
   * Returns available revisions ordered by timestamp
   * @return a Dataset of Revision
   */
  def revisions: Dataset[Revision] =
    snapshot.allFiles
      .select(s"tags.$spaceTag")
      .distinct
      .map(a => JsonUtils.fromJson[Revision](a.getString(0)))
      .orderBy(col("timestamp").desc)

  /**
   * Returns the revision with the higher timestamp
   * @return the revision
   */
  def lastRevision: Revision = {
    // Dataset spaceRevisions is ordered by timestamp
    revisions
      .first()

  }

  /**
   * Returns true if a revision with a specific revision identifier exists
   * @param revisionID the identifier of the revision
   * @return boolean
   */

  def existsRevision(revisionID: RevisionID): Boolean = {
    revisionsData.exists(_.revision.id == revisionID)
  }

  lazy val txnVersion = {
    snapshot.setTransactions.filter(_.appId == indexId) match {
      case Nil => -1
      case list => list.last.version
    }
  }

  lazy val revisionsData: Seq[RevisionData] = {
    revisions
      .collect()
      .map(revision => {
        val revisionFiles = snapshot.allFiles.filter(_.tags(spaceTag) == revision.toString)
        RevisionData(revision, revisionFiles, txnVersion, snapshot.path.getParent)
      })
  }

  lazy val lastRevisionData: RevisionData = {
    getRevisionData(lastRevisionID)
  }

  lazy val lastRevisionID: Long = lastRevision.id

}
