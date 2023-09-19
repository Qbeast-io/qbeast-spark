/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.TableChanges
import io.qbeast.core.model.Weight
import io.qbeast.spark.index.QbeastColumns
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.col

import scala.collection.mutable

/**
 * Implementation of WriteStrategy that groups the records to write by "rolling"
 * them up along the index tree.
 *
 * @param qbeastColumns the Qbeast-specisific columns
 * @param tableChanges the table changes
 */
private[writer] class RollupWriteStrategy(
    qbeastColumns: QbeastColumns,
    tableChanges: TableChanges)
    extends WriteStrategy
    with Serializable {

  override def write(
      data: DataFrame,
      writerFactory: IndexFileWriterFactory): IISeq[(IndexFile, TaskStats)] = {
    data
      .repartition(col(QbeastColumns.cubeColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writeRows(writerFactory, targetCubeIds))
      .collect()
      .toIndexedSeq
  }

  private def writeRows(
      writerFactory: IndexFileWriterFactory,
      targetCubeIds: Map[CubeId, CubeId])(
      rows: Iterator[InternalRow]): Iterator[(IndexFile, TaskStats)] = {
    val writers: mutable.Map[CubeId, IndexFileWriter] = mutable.Map.empty
    rows.foreach { row =>
      val cubeId = getCubeId(row)
      var targetCubeId = targetCubeIds.get(cubeId)
      var parentCubeId = cubeId.parent
      while (targetCubeId.isEmpty) {
        parentCubeId match {
          case Some(value) =>
            targetCubeId = targetCubeIds.get(value)
            parentCubeId = value.parent
          case None => targetCubeId = Some(cubeId)
        }
      }
      val writer = writers.getOrElseUpdate(targetCubeId.get, writerFactory.newWriter())
      writer.write(row)
    }
    writers.values.iterator.map(_.close())
  }

  private def getCubeId(row: InternalRow): CubeId = {
    val bytes = row.getBinary(qbeastColumns.cubeColumnIndex)
    tableChanges.updatedRevision.createCubeId(bytes)
  }

  private def targetCubeIds: Map[CubeId, CubeId] = {
    val minRowsPerFile = tableChanges.updatedRevision.desiredCubeSize.toDouble
    val queue = new mutable.PriorityQueue()(CubeIdOrdering)
    val rollups = mutable.Map.empty[CubeId, Rollup]
    tableChanges.cubeDomains.foreach { case (cubeId, domain) =>
      queue += cubeId
      val minWeight = getMinWeight(cubeId).fraction
      val maxWeight = getMaxWeight(cubeId).fraction
      val size = (maxWeight - minWeight) * domain
      rollups += cubeId -> Rollup(cubeId, size)
    }

    while (queue.nonEmpty) {
      val cubeId = queue.dequeue()
      val rollup = rollups(cubeId)
      if (rollup.size < minRowsPerFile) {
        cubeId.parent match {
          case Some(parentCubeId) =>
            val parentRollup = rollups.get(parentCubeId) match {
              case Some(value) => value
              case None =>
                queue += parentCubeId
                val value = Rollup(parentCubeId, 0)
                rollups += parentCubeId -> value
                value
            }
            parentRollup.append(rollup)
            rollups -= cubeId
          case None => ()
        }
      }
    }

    rollups.flatMap { case (cubeId, rollup) =>
      rollup.cubeIds.map((_, cubeId))
    }.toMap
  }

  private def getMinWeight(cubeId: CubeId): Weight = {
    cubeId.parent match {
      case Some(parentCubeId) => getMaxWeight(parentCubeId)
      case None => Weight.MinValue
    }
  }

  private def getMaxWeight(cubeId: CubeId): Weight = {
    tableChanges.cubeWeight(cubeId).getOrElse(Weight.MaxValue)
  }

}

private object CubeIdOrdering extends Ordering[CubeId] {
  // Cube identifiers are compared by depth in reversed order.
  override def compare(x: CubeId, y: CubeId): Int = y.depth - x.depth
}

private class Rollup(var cubeIds: Seq[CubeId], var size: Double) {

  def append(other: Rollup): Unit = {
    cubeIds ++= other.cubeIds
    size += other.size
  }

}

private object Rollup {
  def apply(cubeId: CubeId, size: Double): Rollup = new Rollup(Seq(cubeId), size)
}
