/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.ColumnsToIndex
import io.qbeast.spark.model.{Revision, RevisionID}
import io.qbeast.spark.sql.utils.TagUtils._
import io.qbeast.spark.index.{CubeId, NormalizedWeight, ReplicatedSet, Weight}
import io.qbeast.spark.model.{CubeInfo, LinearTransformation, SpaceRevision}
import io.qbeast.spark.sql.utils.MetadataConfig
import io.qbeast.spark.sql.utils.State
import io.qbeast.spark.sql.utils.TagUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.{AnalysisExceptionFactory, Dataset, SparkSession}

import scala.collection.immutable.IndexedSeq

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param snapshot the internal Delta Lakes log snapshot
 */
case class QbeastSnapshot(snapshot: Snapshot) {

  def isInitial: Boolean = snapshot.version == -1

  val metadataMap: Map[String, String] = snapshot.metadata.configuration

  val indexedCols: Seq[String] = metadataMap.get(MetadataConfig.indexedColumns) match {
    case Some(json) => JsonUtils.fromJson[Seq[String]](json)
    case None => Seq.empty
  }

  val desiredCubeSize: Int = metadataMap.get(MetadataConfig.desiredCubeSize) match {
    case Some(value) => value.toInt
    case None => 0
  }

  val dimensionCount: Int = indexedCols.length

  private val spark = SparkSession.active
  import spark.implicits._

  /**
   * Looks up for a revision with a certain identifier
   * @param revisionID the ID of the revision
   * @return RevisionData for the corresponding identifier
   **/
  def getRevisionData(revisionID: RevisionID): RevisionData = {
    spaceRevisionsMap
      .getOrElse(revisionID,
        throw AnalysisExceptionFactory.create(s"No space revision available with $revisionID"))



  val replicatedSetsMap: Map[Long, ReplicatedSet] = {
    val listReplicatedSets = metadataMap.filterKeys(_.startsWith(MetadataConfig.replicatedSet))

    listReplicatedSets.map { case (key: String, json: String) =>
      val revisionTimestamp = key.split('.').last.toLong
      val replicatedSet = JsonUtils
        .fromJson[Set[String]](json)
        .map(cube => CubeId(dimensionCount, cube))
      (revisionTimestamp, replicatedSet)
    }
  }

  /**
   * Returns available space revisions ordered by timestamp
   * @return a sequence of SpaceRevision
   */
  val spaceRevisionsMap: Map[Long, Revision] = {
    val listRevisions = metadataMap.filterKeys(_.startsWith(MetadataConfig.revision))

    listRevisions
      .map { case (key: String, json: String) =>
        val revisionTimestamp = key.split('.').last.toLong
        val transformations = JsonUtils
          .fromJson[IndexedSeq[LinearTransformation]](json)
        (revisionTimestamp, Revision(revisionTimestamp, transformations))
      }
  }

  val lastRevisionTimestamp: Long =
    metadataMap.getOrElse(MetadataConfig.lastRevisionTimestamp, "0").toLong

  /**
   * Returns the revision with the higher timestamp
   * @return the revision
   */
  def lastRevision: Revision = {
    getRevisionData(lastRevisionTimestamp).revision
  }
    /**
   * Returns true if a revision with a specific revision identifier exists
   * @param revisionID the identifier of the revision
   * @return boolean
     **/
  def existsRevision(revisionID: RevisionID): Boolean = {
    revisionsData.exists(_.revision.id == revisionID)
  }

    lazy val txnVersion = {
      snapshot.setTransactions.filter(_.appId == indexId) match {
        case Nil => -1
        case list => list.last.version
      }
    }


  }
