/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.IndexFile
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import io.qbeast.IISeq
import io.qbeast.spark.index.QbeastColumns
import org.apache.spark.sql.catalyst.InternalRow
import scala.collection.mutable
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.Revision

/**
 * Legacy write startegy which writes every cube to a separate file.
 */
private[writer] class LegacyWriteStrategy0(revision: Revision)
    extends WriteStrategy0
    with Serializable {

  override def write(
      data: DataFrame,
      writerFactory: IndexFileWriterFactory0): IISeq[(IndexFile, TaskStats)] = {
    val qbeastColumns = QbeastColumns(data)
    data
      .repartition(col(QbeastColumns.cubeColumnName))
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
    rows.foreach { row =>
      val cubeId = getCubeId(qbeastColumns, row)
      val writer = writers.getOrElseUpdate(cubeId, writerFactory.newWriter(qbeastColumns))
      writer.write(row)
    }
    writers.values.iterator.map(_.close())
  }

  private def getCubeId(qbeastColumns: QbeastColumns, row: InternalRow): CubeId = {
    val bytes = row.getBinary(qbeastColumns.cubeColumnIndex)
    revision.createCubeId(bytes)
  }

}
