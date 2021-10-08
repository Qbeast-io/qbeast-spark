/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.QbeastColumns._
import io.qbeast.spark.index.{ColumnsToIndex, CubeId, NormalizedWeight, Weight}
import io.qbeast.spark.model.{CubeInfo, SpaceRevision}
import io.qbeast.spark.sql.utils.State.ANNOUNCED
import io.qbeast.spark.sql.utils.TagUtils._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.{DeltaLogFileIndex, Snapshot}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.sql.{AnalysisExceptionFactory, Dataset, DatasetFactory, SparkSession}

/**
 * Qbeast Snapshot that provides information about the current index state.
 *
 * @param snapshot the internal Delta Lakes log snapshot
 * @param desiredCubeSize the desired cube size
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

  private val logSchema = StructType(
    Array(
      StructField(name = cubeColumnName, dataType = BinaryType, nullable = false),
      StructField(name = revisionColumnName, dataType = LongType, nullable = false)))

  private def fileToDataframe(fileStatus: Array[FileStatus]): Dataset[(Array[Byte], Long)] = {

    val index = DeltaLogFileIndex(new ParquetFileFormat, fileStatus)

    val relation = HadoopFsRelation(
      index,
      index.partitionSchema,
      logSchema,
      None,
      index.format,
      Map.empty[String, String])(spark)

    DatasetFactory.create[(Array[Byte], Long)](spark, LogicalRelation(relation))

  }

  /**
   * Looks up for a space revision with a certain timestamp
   * @param revisionTimestamp the timestamp of the revision
   * @return a SpaceRevision with the corresponding timestamp
   */
  def getRevisionAt(revisionTimestamp: Long): SpaceRevision = {
    spaceRevisions
      .find(_.timestamp.equals(revisionTimestamp))
      .getOrElse(throw AnalysisExceptionFactory.create(
        s"No Revision with id $lastRevisionTimestamp is found"))
  }

  def existsRevision(revisionTimestamp: Long): Boolean = {
    spaceRevisions.exists(_.timestamp.equals(revisionTimestamp))
  }

  /**
   * Returns the cube maximum weights for a given space revision
   * @param revisionTimestamp the timestamp of the revision
   * @return a map with key cube and value max weight
   */
  def cubeWeights(revisionTimestamp: Long): Map[CubeId, Weight] = {
    indexState(revisionTimestamp)
      .collect()
      .map(info => (CubeId(dimensionCount, info.cube), info.maxWeight))
      .toMap
  }

  /**
   * Returns the cube maximum normalized weights for a given space revision
   *
   * @param revisionTimestamp the timestamp of the revision
   * @return a map with key cube and value max weight
   */
  def cubeNormalizedWeights(revisionTimestamp: Long): Map[CubeId, Double] = {
    indexState(revisionTimestamp)
      .collect()
      .map {
        case CubeInfo(cube, Weight.MaxValue, size) =>
          (CubeId(dimensionCount, cube), NormalizedWeight(desiredCubeSize, size))
        case CubeInfo(cube, maxWeight, _) =>
          (CubeId(dimensionCount, cube), NormalizedWeight(maxWeight))
      }
      .toMap
  }

  /**
   * Returns the set of cubes that are overflowed for a given space revision
   * @param revisionTimestamp the timestamp of the revision
   * @return the set of overflowed cubes
   */

  def overflowedSet(revisionTimestamp: Long): Set[CubeId] = {
    indexState(revisionTimestamp)
      .filter(_.maxWeight != Weight.MaxValue)
      .collect()
      .map(cubeInfo => CubeId(dimensionCount, cubeInfo.cube))
      .toSet
  }

  /**
   * Returns the replicated set for a given space revision
   * @param revisionTimestamp the timestamp of the revision
   * @return the set of cubes in a replicated state
   */
  def replicatedSet(revisionTimestamp: Long): Set[CubeId] = {

    val hadoopConf = spark.sessionState.newHadoopConf()

    snapshot.setTransactions.filter(_.appId.equals(indexId)) match {

      case Nil => Set.empty[CubeId]

      case list =>
        val txn = list.last
        val filePath = new Path(snapshot.path.getParent, s"_qbeast/${txn.version}")
        val fs = snapshot.path.getFileSystem(hadoopConf)
        if (fs.exists(filePath)) {
          val fileStatus = fs.listStatus(filePath)
          fileToDataframe(fileStatus)
            .filter(_._2.equals(revisionTimestamp))
            .collect()
            .map(r => CubeId(dimensionCount, r._1))
            .toSet
        } else Set.empty[CubeId]

    }
  }

  lazy val metadataMap = snapshot.metadata.configuration

  /**
   * Returns available space revisions ordered by timestamp
   * @return a Dataset of SpaceRevision
   */
  lazy val spaceRevisions: Seq[SpaceRevision] = {
    metadataMap.get("qb.revisions") match {
      case Some(jsonSpaceRevisions) =>
        JsonUtils
          .fromJson[Seq[SpaceRevision]](jsonSpaceRevisions)
      case None => Seq.empty
    }
  }

  lazy val lastRevisionTimestamp = metadataMap("qb.lastRevisionId").toLong

  /**
   * Returns the space revision with the higher timestamp
   * @return the space revision
   */
  lazy val lastSpaceRevision: SpaceRevision = {
    getRevisionAt(lastRevisionTimestamp)
  }

  /**
   * Returns the index state for the given space revision
   * @param revisionTimestamp the timestamp of the revision
   * @return Dataset containing cube information
   */
  private def indexState(revisionTimestamp: Long): Dataset[CubeInfo] = {

    val allFiles = snapshot.allFiles
    val weightValueTag = weightMaxTag + ".value"

    allFiles
      .filter(_.tags(spaceTag).equals(revisionTimestamp.toString))
      .map(a =>
        BlockStats(
          a.tags(cubeTag),
          Weight(a.tags(weightMaxTag).toInt),
          Weight(a.tags(weightMinTag).toInt),
          a.tags(stateTag),
          a.tags(elementCountTag).toLong))
      .groupBy(cubeTag)
      .agg(min(weightValueTag), sum(elementCountTag))
      .map(row => CubeInfo(row.getAs[String](0), Weight(row.getAs[Int](1)), row.getAs[Long](2)))
  }

  /**
   * Returns the sequence of blocks for a set of cubes belonging to a specific space revision
   * @param cubes the set of cubes
   * @param revisionTimestamp the timestamp of the revision
   * @return the sequence of blocks
   */
  def getCubeBlocks(cubes: Set[CubeId], revisionTimestamp: Long): Seq[AddFile] = {
    val dimensionCount = this.dimensionCount
    snapshot.allFiles
      .filter(_.tags(spaceTag).equals(revisionTimestamp.toString))
      .filter(_.tags(stateTag) != ANNOUNCED)
      .filter(a => cubes.contains(CubeId(dimensionCount, a.tags(cubeTag))))
      .collect()
  }

}
