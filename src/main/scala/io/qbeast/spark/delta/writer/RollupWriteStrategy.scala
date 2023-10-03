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
 * @param writerFactory the writer factory
 * @param tableChanges the table changes
 */
private[writer] class RollupWriteStrategy(
    writerFactory: IndexFileWriterFactory,
    tableChanges: TableChanges)
    extends WriteStrategy
    with Serializable {

  private type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]
  private type Extract = InternalRow => (InternalRow, CubeId, Weight, CubeId)

  override def write(data: DataFrame): IISeq[(IndexFile, TaskStats)] = {
    val dataWithRollup = addRollup(data)
    val writeRows = getWriteRows(dataWithRollup)
    dataWithRollup
      .repartition(col(QbeastColumns.cubeToRollupColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writeRows)
      .collect()
      .toIndexedSeq
  }

  private def addRollup(data: DataFrame): DataFrame = data.withColumn(
    QbeastColumns.cubeToRollupColumnName,
    getRollupCubeIdUDF(computeRollupCubeIds)(col(QbeastColumns.cubeColumnName)))

  private def getWriteRows(data: DataFrame): WriteRows = {
    val extract = getExtract(data)
    rows => {
      val writers = mutable.Map.empty[CubeId, IndexFileWriter]
      rows.foreach { indexedRow =>
        val (row, cubeId, weight, rollupCubeId) = extract(indexedRow)
        val writer = writers.getOrElseUpdate(rollupCubeId, writerFactory.createIndexFileWriter())
        writer.write(row, cubeId, weight)
      }
      writers.values.iterator.map(_.close())
    }
  }

  private def getExtract(data: DataFrame): Extract = {
    val schema = data.schema
    val qbeastColumns = QbeastColumns(data)
    val extractors = (0 until schema.fields.length)
      .filterNot(qbeastColumns.contains)
      .map { i => row: InternalRow => row.get(i, schema(i).dataType) }
      .toSeq
    indexedRow => {
      val row = InternalRow.fromSeq(extractors.map(_.apply(indexedRow))).copy()
      val cubeIdBytes = indexedRow.getBinary(qbeastColumns.cubeColumnIndex)
      val cubeId = tableChanges.updatedRevision.createCubeId(cubeIdBytes)
      val weight = Weight(indexedRow.getInt(qbeastColumns.weightColumnIndex))
      val rollupCubeIdBytes = indexedRow.getBinary(qbeastColumns.cubeToRollupColumnIndex)
      val rollupCubeId = tableChanges.updatedRevision.createCubeId(rollupCubeIdBytes)
      (row, cubeId, weight, rollupCubeId)
    }
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

  private object Rollup extends Serializable {
    def apply(cubeId: CubeId, size: Double): Rollup = new Rollup(Seq(cubeId), size)
  }

}
