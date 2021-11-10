/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model.{CubeId, IndexStatus, QTableID, TableChanges}
import io.qbeast.spark.index.OTreeAlgorithm
import io.qbeast.spark.index.QbeastColumns.cubeToReplicateColumnName
import io.qbeast.spark.utils.{TagUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.{AddFile}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOptions}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * QbeastOptimizer is in charge of optimizing the index
 *
 * @param deltaLog       deltaLog of the index
 * @param deltaOptions   deltaOptions for writing on the index
 * @param oTreeAlgorithm algorithm to replicate data
 * @param metadataManager metadataManager for the index
 */
class QbeastOptimizer(
    tableId: QTableID,
    deltaLog: DeltaLog,
    deltaOptions: DeltaOptions,
    indexStatus: IndexStatus,
    oTreeAlgorithm: OTreeAlgorithm,
    metadataManager: SparkDeltaMetadataManager)
    extends QbeastMetadataOperation {

  /**
   * Performs the optimization
   *
   * @param sparkSession   SparkSession
   * @param cubesToOptimize list of cubes to optimize
   * @return the set of cubes that completed the operation along with the file actions to commit
   */
  def optimize(
      sparkSession: SparkSession,
      cubesToOptimize: Set[CubeId]): (DataFrame, TableChanges) = {

    val replicatedSet = indexStatus.replicatedSet
    val cubesToReplicate =
      cubesToOptimize.diff(replicatedSet)
    val revision = indexStatus.revision

    val cubeBlocks = metadataManager
      .getCubeFiles(tableId, indexStatus, cubesToReplicate)
      .groupBy(_.tags(TagUtils.cube))

    val dataToReplicate = cubeBlocks
      .map { case (cube: String, blocks: Seq[AddFile]) =>
        val cubeId = revision.createCubeId(cube)
        sparkSession.read
          .format("parquet")
          .load(blocks.map(f => new Path(tableId.id, f.path).toString): _*)
          .withColumn(cubeToReplicateColumnName, lit(cubeId.bytes))
      }
      .reduce(_ union _)

    val (dataFrame, tc) = oTreeAlgorithm.replicateCubes(dataToReplicate, indexStatus)
    val indexChangesWithReplicatedSet =
      tc.indexChanges.copy(deltaReplicatedSet = cubesToReplicate)
    (dataFrame, tc.copy(indexChanges = indexChangesWithReplicatedSet))

  }

}
