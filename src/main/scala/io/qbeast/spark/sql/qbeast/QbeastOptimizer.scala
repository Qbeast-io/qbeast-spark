/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.QbeastColumns.{cubeToReplicateColumnName}
import io.qbeast.spark.index.{CubeId, OTreeAlgorithm}
import io.qbeast.spark.sql.utils.State.REPLICATED
import io.qbeast.spark.sql.utils.TagUtils.{cubeTag, stateTag}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.{Action, AddFile, FileAction}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions, OptimisticTransaction}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{BinaryType, StructField}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._

/**
 * QbeastOptimizer is in charge of optimizing the index
 *
 * @param deltaLog       deltaLog of the index
 * @param deltaOptions   deltaOptions for writing on the index
 * @param qbeastSnapshot current snapshot of the OTree
 * @param revisionTimestamp timestamp of the revision to optimize
 * @param oTreeAlgorithm algorithm to replicate data
 */
class QbeastOptimizer(
    deltaLog: DeltaLog,
    deltaOptions: DeltaOptions,
    qbeastSnapshot: QbeastSnapshot,
    revisionTimestamp: Long,
    oTreeAlgorithm: OTreeAlgorithm)
    extends QbeastMetadataOperation {

  /**
   * Performs the optimization
   *
   * @param sparkSession   SparkSession
   * @param announcedCubes Set of cube paths announced
   * @return the set of cubes that completed the operation along with the file actions to commit
   */
  def optimize(
      txn: OptimisticTransaction,
      sparkSession: SparkSession,
      announcedCubes: Set[String]): (Set[CubeId], Seq[Action]) = {

    val schema =
      qbeastSnapshot.snapshot.schema.add(
        StructField(cubeToReplicateColumnName, BinaryType, false))
    val emptyDataFrame = sparkSession.createDataFrame(List.empty[Row].asJava, schema)

    val dimensionCount = qbeastSnapshot.dimensionCount
    val replicatedSet = qbeastSnapshot.replicatedSet(revisionTimestamp)

    val cubesToOptimize = announcedCubes.map(CubeId(dimensionCount, _))
    val cubesToReplicate =
      cubesToOptimize.diff(replicatedSet)

    val dataPath = qbeastSnapshot.snapshot.path.getParent
    val (dataToReplicate, updatedActions) = qbeastSnapshot
      .getCubeBlocks(cubesToReplicate, revisionTimestamp)
      .groupBy(_.tags(cubeTag))
      .map { case (cube: String, blocks: Seq[AddFile]) =>
        val cubeId = CubeId(dimensionCount, cube)
        val data = sparkSession.read
          .format("parquet")
          .load(blocks.map(f => new Path(dataPath, f.path).toString): _*)
          .withColumn(cubeToReplicateColumnName, lit(cubeId.bytes))

        val newAddFiles = blocks
          .map(f => f.copy(tags = f.tags.updated(stateTag, REPLICATED)))
        val deletedFiles = blocks.map(_.remove)

        (data, deletedFiles ++ newAddFiles)
      }
      .foldLeft((emptyDataFrame, Seq.empty[FileAction]))((a, b) =>
        (a._1.union(b._1), a._2 ++ b._2))

    val writer = QbeastWriter(
      mode = SaveMode.Append,
      deltaLog = deltaLog,
      options = deltaOptions,
      partitionColumns = Nil,
      data = dataToReplicate,
      columnsToIndex = qbeastSnapshot.indexedCols,
      qbeastSnapshot = qbeastSnapshot,
      announcedSet = cubesToOptimize,
      oTreeAlgorithm = oTreeAlgorithm)

    val spaceRevision = qbeastSnapshot.lastSpaceRevision
    val (qbeastData, weightMap) = oTreeAlgorithm
      .replicateCubes(dataToReplicate, spaceRevision, qbeastSnapshot, cubesToOptimize)

    // updateMetadata
    updateQbeastReplicatedSet(txn, spaceRevision.timestamp, qbeastSnapshot, cubesToOptimize)

    // write files
    val addFiles = writer.writeFiles(qbeastData, spaceRevision, weightMap)
    val actions = addFiles ++ updatedActions
    (cubesToReplicate, actions)

  }

}
