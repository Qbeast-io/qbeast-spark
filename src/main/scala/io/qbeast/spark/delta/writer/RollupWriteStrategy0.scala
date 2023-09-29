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
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

/**
 * Implementation of WriteStrategy that groups the records to write by "rolling"
 * them up along the index tree.
 *
 * @param qbeastColumns the Qbeast-specisific columns
 * @param tableChanges the table changes
 */
private[writer] class RollupWriteStrategy0(tableChanges: TableChanges)
    extends WriteStrategy0
    with Serializable {

  override def write(
      data: DataFrame,
      writerFactory: IndexFileWriterFactory0): IISeq[(IndexFile, TaskStats)] = {
    val dataWithRollup = data
      .withColumn(
        QbeastColumns.cubeToRollupColumnName,
        getRollupCubeIdUDF(computeRollupCubeIds)(col(QbeastColumns.cubeColumnName)))
    val qbeastColumns = QbeastColumns(dataWithRollup.schema)
    dataWithRollup
      .repartition(col(QbeastColumns.cubeToRollupColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writeRows(writerFactory, qbeastColumns))
      .collect()
      .toIndexedSeq
  }

  private def writeRows(writerFactory: IndexFileWriterFactory0, qbeastColumns: QbeastColumns)(
      rows: Iterator[InternalRow]): Iterator[(IndexFile, TaskStats)] = {
    val writers: mutable.Map[CubeId, IndexFileWriter0] = mutable.Map.empty
    val buffers: mutable.Map[CubeId, mutable.Buffer[InternalRow]] = mutable.Map.empty
    val limit = tableChanges.updatedRevision.desiredCubeSize / 2
    rows.foreach { row =>
      val cubeId = getCubeId(qbeastColumns, row)
      val buffer = buffers.getOrElseUpdate(cubeId, mutable.Buffer.empty)
      buffer += row.copy()
      if (buffer.size >= limit) {
        flushBuffer(writerFactory, qbeastColumns, writers, buffer)
      }
    }
    buffers.values.foreach { buffer =>
      flushBuffer(writerFactory, qbeastColumns, writers, buffer)
    }
    writers.values.iterator.map(_.close())
  }

  private def getCubeId(qbeastColumns: QbeastColumns, row: InternalRow): CubeId = {
    val bytes = row.getBinary(qbeastColumns.cubeColumnIndex)
    tableChanges.updatedRevision.createCubeId(bytes)
  }

  private def flushBuffer(
      writerFactory: IndexFileWriterFactory0,
      qbeastColumns: QbeastColumns,
      writers: mutable.Map[CubeId, IndexFileWriter0],
      buffer: mutable.Buffer[InternalRow]): Unit = {
    buffer.foreach { row =>
      val cubeId = getRollupCubeId(qbeastColumns, row)
      val writer = writers.getOrElseUpdate(cubeId, writerFactory.newWriter(qbeastColumns))
      writer.write(row)
    }
    buffer.clear()
  }

  private def getRollupCubeIdUDF(rollupCubeIds: Map[CubeId, CubeId]): UserDefinedFunction =
    udf((cubeIdBytes: Array[Byte]) => {
      val cubeId = tableChanges.updatedRevision.createCubeId(cubeIdBytes)
      var rollupCubeId = rollupCubeIds.get(cubeId)
      var parentCubeId = cubeId.parent
      while (rollupCubeId.isEmpty) {
        parentCubeId match {
          case Some(value) =>
            rollupCubeId = rollupCubeIds.get(value)
            parentCubeId = value.parent
          case None => rollupCubeId = Some(cubeId)
        }
      }
      rollupCubeId.get.bytes
    })

  private def getRollupCubeId(qbeastColumns: QbeastColumns, row: InternalRow): CubeId = {
    val bytes = row.getBinary(qbeastColumns.cubeToRollupColumnIndex)
    tableChanges.updatedRevision.createCubeId(bytes)
  }

  private def computeRollupCubeIds: Map[CubeId, CubeId] = {
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
