/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model.{CubeId, RevisionID}
import io.qbeast.spark.index.OTreeAlgorithm
import io.qbeast.spark.index.QbeastColumns.cubeToReplicateColumnName
import io.qbeast.spark.utils.{State, TagUtils}
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
 * @param revisionID  identifier of revision to optimize
 * @param oTreeAlgorithm algorithm to replicate data
 */
class QbeastOptimizer(
    deltaLog: DeltaLog,
    deltaOptions: DeltaOptions,
    qbeastSnapshot: QbeastSnapshot,
    revisionID: RevisionID,
    oTreeAlgorithm: OTreeAlgorithm)
    extends QbeastMetadataOperation {

  private def revisionData = qbeastSnapshot.getRevisionData(revisionID)
  private def revision = revisionData.revision

  /**
   * Performs the optimization
   *
   * @param txn the OptimisticTransaction
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

    val dimensionCount = revisionData.revision.dimensionCount
    val replicatedSet = revisionData.replicatedSet

    val cubesToOptimize = announcedCubes.map(CubeId(dimensionCount, _))
    val cubesToReplicate =
      cubesToOptimize.diff(replicatedSet)

    val dataPath = qbeastSnapshot.snapshot.path.getParent
    val (dataToReplicate, updatedActions) = revisionData
      .getCubeBlocks(cubesToReplicate)
      .groupBy(_.tags(TagUtils.cube))
      .map { case (cube: String, blocks: Seq[AddFile]) =>
        val cubeId = CubeId(dimensionCount, cube)
        val data = sparkSession.read
          .format("parquet")
          .load(blocks.map(f => new Path(dataPath, f.path).toString): _*)
          .withColumn(cubeToReplicateColumnName, lit(cubeId.bytes))

        val newAddFiles = blocks
          .map(f => f.copy(tags = f.tags.updated(TagUtils.state, State.REPLICATED)))
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
      columnsToIndex = revision.indexedColumns,
      qbeastSnapshot = qbeastSnapshot,
      announcedSet = cubesToOptimize,
      oTreeAlgorithm = oTreeAlgorithm)

    val (qbeastData, weightMap) = oTreeAlgorithm
      .replicateCubes(dataToReplicate, revisionData, cubesToOptimize)
    // updateMetadata
    updateQbeastReplicatedSet(txn, revisionData, cubesToOptimize)

    // write files
    val addFiles = writer.writeFiles(qbeastData, revision, weightMap)
    val actions = addFiles ++ updatedActions
    (cubesToReplicate, actions)

  }

}
