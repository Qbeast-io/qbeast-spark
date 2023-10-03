/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.{CubeId, Weight}
import org.apache.spark.sql.catalyst.InternalRow

import io.qbeast.core.model.IndexFile
import org.apache.spark.sql.execution.datasources.WriteTaskStatsTracker
import io.qbeast.core.model.TableChanges
import io.qbeast.spark.utils.State

/**
 * Default implementation of the IndexFileWriter.
 *
 * @param generator the index file generator
 * @param trackers the task stats trackers
 * @param tableChanges the table changes
 */
private[writer] class DefaultIndexFileWriter(
    generator: IndexFileGenerator,
    trackers: Seq[WriteTaskStatsTracker],
    tableChanges: TableChanges)
    extends IndexFileWriter {

  private val path = generator.path
  private var blockCubeId: Option[CubeId] = None
  private var blockMinWeight: Option[Weight] = None

  trackers.foreach(_.newFile(path))

  override def write(row: InternalRow, cubeId: CubeId, weight: Weight): Unit =
    blockCubeId match {
      case Some(value) if value == cubeId =>
        writeRow(row, weight)
      case Some(_) =>
        endBlock()
        beginBlock(cubeId)
        writeRow(row, weight)
      case None =>
        beginBlock(cubeId)
        writeRow(row, weight)
    }

  override def close(): (IndexFile, TaskStats) = {
    if (blockCubeId.isDefined) {
      endBlock()
    }
    val file = generator.close()
    trackers.foreach(_.closeFile(generator.path))
    val time = System.currentTimeMillis()
    val stats = TaskStats(trackers.map(_.getFinalStats(time)), time)
    (file, stats)
  }

  private def beginBlock(cubeId: CubeId): Unit = {
    val state = tableChanges.cubeState(cubeId).getOrElse(State.FLOODED)
    generator.beginBlock(cubeId, state)
    blockCubeId = Some(cubeId)
  }

  private def writeRow(row: InternalRow, weight: Weight): Unit = {
    generator.writeRow(row)
    blockMinWeight = blockMinWeight match {
      case Some(value) => Some(Weight.min(value, weight))
      case None => Some(weight)
    }
    trackers.foreach(_.newRow(generator.path, row))
  }

  private def endBlock(): Unit = {
    val minWeight = blockMinWeight.getOrElse(Weight.MinValue)
    val maxWeight = tableChanges.cubeWeight(blockCubeId.get).getOrElse(Weight.MaxValue)
    generator.endBlock(minWeight, maxWeight)
    blockCubeId = None
    blockMinWeight = None
  }

}
