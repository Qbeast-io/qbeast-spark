/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, revisionColumnName}
import io.qbeast.spark.index.{CubeId, NormalizedWeight, Weight}
import io.qbeast.spark.model.{CubeInfo, Revision}
import io.qbeast.spark.sql.utils.State.ANNOUNCED
import io.qbeast.spark.sql.utils.TagUtils._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.delta.DeltaLogFileIndex
import org.apache.spark.sql.{Dataset, DatasetFactory, SparkSession}
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{min, sum}
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}

/**
 * Snapshot that provides information about the current index state of a given Revision.
 * @param revision the given Revision
 * @param files dataset of AddFiles that belongs to the specific Revision
 * @param txnVersion the transaction version of the replicated set
 * @param dataPath the data path
 */
case class RevisionData(
    revision: Revision,
    files: Dataset[AddFile],
    txnVersion: Long,
    dataPath: Path) {

  private def dimensionCount = revision.dimensionCount

  /**
   * Returns the cube maximum weights for a given space revision
   * @return a map with key cube and value max weight
   */
  def cubeWeights: Map[CubeId, Weight] = {
    revisionState
      .collect()
      .map(info => (CubeId(dimensionCount, info.cube), info.maxWeight))
      .toMap
  }

  /**
   * Returns the cube maximum normalized weights for a given space revision
   *
   * @return a map with key cube and value max weight
   */
  def cubeNormalizedWeights: Map[CubeId, Double] = {
    revisionState
      .collect()
      .map {
        case CubeInfo(cube, Weight.MaxValue, size) =>
          (
            CubeId(revision.dimensionCount, cube),
            NormalizedWeight(revision.desiredCubeSize, size))
        case CubeInfo(cube, maxWeight, _) =>
          (CubeId(dimensionCount, cube), NormalizedWeight(maxWeight))
      }
      .toMap
  }

  /**
   * Returns the set of cubes that are overflowed for a given space revision
   * @return the set of overflowed cubes
   */

  def overflowedSet: Set[CubeId] = {
    revisionState
      .filter(_.maxWeight != Weight.MaxValue)
      .collect()
      .map(cubeInfo => CubeId(dimensionCount, cubeInfo.cube))
      .toSet
  }

  /**
   * Returns the index state for the given space revision
   * @return Dataset containing cube information
   */
  private def revisionState: Dataset[CubeInfo] = {

    val spark = SparkSession.active
    import spark.implicits._

    val weightValueTag = weightMaxTag + ".value"

    files
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
   * @return the sequence of blocks
   */
  def getCubeBlocks(cubes: Set[CubeId]): Seq[AddFile] = {
    files
      .filter(_.tags(stateTag) != ANNOUNCED)
      .filter(a => cubes.contains(CubeId(dimensionCount, a.tags(cubeTag))))
      .collect()
  }

  private val logSchema = StructType(
    Array(
      StructField(name = cubeColumnName, dataType = BinaryType, nullable = false),
      StructField(name = revisionColumnName, dataType = LongType, nullable = false)))

  private def fileToDataframe(fileStatus: Array[FileStatus]): Dataset[(Array[Byte], Long)] = {

    val spark = SparkSession.active
    import spark.implicits._

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
   * Returns the replicated set for the revision
   * @return the set of cubes in a replicated state
   */
  def replicatedSet: Set[CubeId] = {

    val spark = SparkSession.active
    val hadoopConf = spark.sessionState.newHadoopConf()

    val filePath = new Path(dataPath, s"_qbeast/$txnVersion")
    val fs = dataPath.getFileSystem(hadoopConf)
    if (fs.exists(filePath)) {
      val fileStatus = fs.listStatus(filePath)
      fileToDataframe(fileStatus)
        .filter(_._2.equals(revision.timestamp))
        .collect()
        .map(r => CubeId(dimensionCount, r._1))
        .toSet
    } else Set.empty[CubeId]

  }

}
