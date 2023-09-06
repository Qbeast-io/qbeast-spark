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
private[writer] class LegacyWriteStrategy(revision: Revision, qbeastColumns: QbeastColumns)
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
      .mapPartitions(writeRows(writerFactory))
      .collect()
      .toIndexedSeq
  }

  private def writeRows(writerFactory: IndexFileWriterFactory)(
      rows: Iterator[InternalRow]): Iterator[(IndexFile, TaskStats)] = {
    val writers: mutable.Map[CubeId, IndexFileWriter] = mutable.Map.empty
    rows.foreach { row =>
      val cubeId = getCubeId(row)
      val writer = writers.getOrElseUpdate(cubeId, writerFactory.newWriter())
      writer.write(row)
    }
    writers.values.iterator.map(_.close())
  }

  private def getCubeId(row: InternalRow): CubeId = {
    val bytes = row.getBinary(qbeastColumns.cubeColumnIndex)
    revision.createCubeId(bytes)
  }

}
