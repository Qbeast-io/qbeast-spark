/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.writer

import io.qbeast.IISeq
import io.qbeast.model.{DataWriter, QTableID, TableChanges}
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.index.QbeastColumns.{cubeColumnName, stateColumnName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Spark implementation of the DataWriter interface.
 */
object SparkDataWriter extends DataWriter[DataFrame, StructType, FileAction] {

  override def write(
      qtable: QTableID,
      schema: StructType,
      qbeastData: DataFrame,
      tableChanges: TableChanges): IISeq[FileAction] = {

    val sparkSession = qbeastData.sparkSession

    val (factory: OutputWriterFactory, serConf: SerializableConfiguration) = {
      val format = new ParquetFileFormat()
      val job = Job.getInstance()
      (
        format.prepareWrite(sparkSession, job, Map.empty, schema),
        new SerializableConfiguration(job.getConfiguration))
    }

    val qbeastColumns = QbeastColumns(qbeastData)

    val blockWriter =
      BlockWriter(
        dataPath = qtable.id,
        schema = schema,
        schemaIndex = qbeastData.schema,
        factory = factory,
        serConf = serConf,
        qbeastColumns = qbeastColumns,
        tableChanges = tableChanges)
    qbeastData
      .repartition(col(cubeColumnName), col(stateColumnName))
      .queryExecution
      .executedPlan
      .execute
      .mapPartitions(blockWriter.writeRow)
      .collect()
      .toIndexedSeq

  }

}
