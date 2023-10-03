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
 * WriteStrategy implementation that uses legacy approach to write every cube
 * block into a separate file.
 *
 * @param writerFactory the writer factory
 * @param tableChanges the table changes
 */
private[writer] class LegacyWriteStrategy(
    writerFactory: IndexFileWriterFactory,
    tableChanges: TableChanges)
    extends WriteStrategy
    with Serializable {

  private type WriteRows = Iterator[InternalRow] => Iterator[(IndexFile, TaskStats)]
  private type Extract = InternalRow => (InternalRow, CubeId, Weight)

  override def write(data: DataFrame): IISeq[(IndexFile, TaskStats)] = {
    val writeRows = getWriteRows(data)
    data
      .repartition(col(QbeastColumns.cubeColumnName))
      .queryExecution
      .executedPlan
      .execute()
      .mapPartitions(writeRows)
      .collect()
      .toIndexedSeq
  }

  private def getWriteRows(data: DataFrame): WriteRows = {
    val extract = getExtract(data)
    rows => {
      val writers = mutable.Map.empty[CubeId, IndexFileWriter]
      rows.foreach { indexedRow =>
        val (row, cubeId, weight) = extract(indexedRow)
        val writer = writers.getOrElseUpdate(cubeId, writerFactory.createIndexFileWriter())
        writer.write(row, cubeId, weight)
      }
      writers.values.iterator.map(_.close())
    }
  }

  private def getExtract(data: DataFrame): Extract = {
    val schema = data.schema
    val qbeastColumns = QbeastColumns(data)
    val extractors = (0 until schema.fields.length).iterator
      .filterNot(qbeastColumns.contains)
      .map(i => { row: InternalRow => row.get(i, schema(i).dataType) })
      .toSeq
    indexedRow: InternalRow => {
      val row = InternalRow.fromSeq(extractors.map(_.apply(indexedRow)))
      val bytes = indexedRow.getBinary(qbeastColumns.cubeColumnIndex)
      val cubeId = tableChanges.updatedRevision.createCubeId(bytes)
      val weight = Weight(indexedRow.getInt(qbeastColumns.weightColumnIndex))
      (row, cubeId, weight)
    }
  }

}
