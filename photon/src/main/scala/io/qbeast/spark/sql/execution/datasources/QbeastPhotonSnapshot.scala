package io.qbeast.spark.sql.execution.datasources

import io.delta.tables.DeltaTable
import io.qbeast.IISeq
import io.qbeast.core.model._
import io.qbeast.spark.sql.utils.QbeastMetadataSQL.{
  createCube,
  normalizeWeight,
  qBlock,
  qbeastBlock,
  weight
}
import io.qbeast.spark.utils.MetadataConfig
import org.apache.spark.sql.functions.{col, collect_list, lit, min, sum}
import org.apache.spark.sql.{AnalysisException, AnalysisExceptionFactory, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import scala.collection.immutable.SortedMap

/**
 * QbeastSnapshot for Photon
 * @param sparkSession the current spark session
 * @param path the path of the table
 */
case class QbeastPhotonSnapshot(sparkSession: SparkSession, path: String)
    extends QbeastSnapshot
    with StagingUtils {

  private lazy val deltaTable = {
    try {
      DeltaTable.forPath(sparkSession, path)
    } catch {
      case e: AnalysisException => throw e.copy(message = s"$path is not a Qbeast Table")
    }
  }

  private lazy val deltaLog = deltaTable.getClass.getMethod("deltaLog").invoke(deltaTable)
  private lazy val snapshot = deltaLog.getClass.getMethod("snapshot").invoke(deltaLog)

  private val allFiles = snapshot.getClass
    .getMethod("allFiles")
    .invoke(snapshot)
    .asInstanceOf[DataFrame]

  private val metadata = snapshot.getClass.getMethod("metadata").invoke(snapshot)

  private val metadataMap = metadata.getClass
    .getMethod("configuration")
    .invoke(metadata)
    .asInstanceOf[Map[String, String]]

  /**
   * Constructs revision dictionary
   *
   * @return a map of revision identifier and revision
   */

  private val revisionsMap: Map[RevisionID, Revision] = {
    val listRevisions = metadataMap.filterKeys(_.startsWith(MetadataConfig.revision))
    listRevisions.map { case (key: String, json: String) =>
      val revisionID = key.split('.').last.toLong
      val revision = mapper
        .readValue[Revision](json, classOf[Revision])
      (revisionID, revision)
    }
  }

  /**
   * Constructs replicated set for each revision
   *
   * @return a map of revision identifier and replicated set
   */

  private val replicatedSetsMap: Map[RevisionID, ReplicatedSet] = {
    val listReplicatedSets = metadataMap.filterKeys(_.startsWith(MetadataConfig.replicatedSet))

    listReplicatedSets.map { case (key: String, json: String) =>
      val revisionID = key.split('.').last.toLong
      val revision = loadRevision(revisionID)
      val replicatedSet = mapper
        .readValue[Set[String]](json, classOf[Set[String]])
        .map(revision.createCubeId)
      (revisionID, replicatedSet)
    }
  }

  /**
   * Returns last available revision identifier
   *
   * @return revision identifier
   */
  private val lastRevisionID: RevisionID =
    metadataMap.getOrElse(MetadataConfig.lastRevisionID, "-1").toLong

  /**
   * Checks if the table exists
   * @return
   */

  private def tableExists(): Boolean =
    deltaLog.getClass.getMethod("tableExists").invoke(deltaLog).asInstanceOf[Boolean]

  /**
   * Gets the schema of the table from the Delta Snapshot
   * @return schema
   */
  private def getSchema(): StructType =
    snapshot.getClass.getMethod("schema").invoke(snapshot).asInstanceOf[StructType]

  /**
   * Returns the replicated set for a revision identifier if exists
   *
   * @param revisionID the revision identifier
   * @return the replicated set
   */
  private def getReplicatedSet(revisionID: RevisionID): ReplicatedSet = {
    replicatedSetsMap
      .getOrElse(revisionID, Set.empty)
  }

  /**
   * Returns the schema of the table
   * @return schema
   */
  def schema: StructType = if (tableExists()) getSchema()
  else {
    throw AnalysisExceptionFactory.create("this table does not exists")
  }

  override def isInitial: Boolean =
    snapshot.getClass.getMethod("version").invoke(snapshot).asInstanceOf[Long] == -1

  override def loadLatestIndexStatus: IndexStatus = loadIndexStatus(lastRevisionID)

  override def loadIndexStatus(revisionID: RevisionID): IndexStatus = {
    import sparkSession.implicits._

    val revision = loadRevision(revisionID)
    val builder = SortedMap.newBuilder[CubeId, CubeStatus]

    val ndims = revision.columnTransformers.size
    val desiredCubeSize = revision.desiredCubeSize
    val revisionFiles = allFiles.filter(s"tags.revision == $revisionID")

    revisionFiles
      .groupBy(col("tags.cube"))
      .agg(
        min(weight(col("tags.maxWeight"))).as("maxWeight"),
        sum(col("tags.elementCount")).as("elementCount"),
        collect_list(qbeastBlock).as("files"))
      .select(
        createCube(col("cube"), lit(ndims)).as("cubeId"),
        col("maxWeight"),
        normalizeWeight(col("maxWeight"), col("elementCount"), lit(desiredCubeSize)).as(
          "normalizedWeight"),
        col("files"))
      .as[CubeStatus]
      .collect()
      .foreach(row => builder += row.cubeId -> row)

    val cubeStatus = builder.result()
    val replicatedSet = getReplicatedSet(revisionID)
    val announcedSet = Set.empty[CubeId]

    IndexStatus(
      revision = revision,
      replicatedSet = replicatedSet,
      announcedSet = announcedSet,
      cubesStatuses = cubeStatus)

  }

  override def loadAllRevisions: IISeq[Revision] =
    revisionsMap.keys.map(loadRevision).toIndexedSeq

  override def loadLatestRevision: Revision = loadRevision(lastRevisionID)

  override def loadRevision(revisionID: RevisionID): Revision = {
    revisionsMap
      .getOrElse(
        revisionID,
        throw AnalysisExceptionFactory.create(s"No space revision available with $revisionID"))
  }

  override def loadRevisionAt(timestamp: Long): Revision = {
    val candidateRevisions = revisionsMap.values.filter(_.timestamp <= timestamp)
    if (candidateRevisions.nonEmpty) candidateRevisions.maxBy(_.timestamp)
    else {
      throw AnalysisExceptionFactory
        .create(s"No space revision available before $timestamp")
    }
  }

  /**
   * Returns all QbeastBlocks presents on the table
   * @return a sequence of QbeastBlocks
   */

  def allBlocks(): Seq[QbeastBlock] = {
    import sparkSession.implicits._
    allFiles.filter("tags is not null").select(qBlock: _*).as[QbeastBlock].collect()
  }

}
