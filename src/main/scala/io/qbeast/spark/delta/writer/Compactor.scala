/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model.{CubeId, QTableID, QbeastBlock, TableChanges}
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, TaskAttemptContextImpl, TaskAttemptID}
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.{AddFile, FileAction, RemoveFile}
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID

case class Compactor(
    tableID: QTableID,
    factory: OutputWriterFactory,
    serConf: SerializableConfiguration,
    schema: StructType,
    cubeId: CubeId,
    cubeBlocks: IISeq[QbeastBlock],
    tableChanges: TableChanges)
    extends Serializable {

  def writeBlock(it: Iterator[InternalRow]): Iterator[FileAction] = {

    val minWeight = cubeBlocks.map(_.minWeight).min
    val maxWeight =
      cubeBlocks.map(_.maxWeight).min // we pick the lesser updated value
    val state = tableChanges.cubeState(cubeId).getOrElse(State.FLOODED)
    val rowCount = cubeBlocks.map(_.elementCount).sum
    val revision = tableChanges.updatedRevision

    // Update the tags of the block with the information of the cubeBlocks
    val tags: Map[String, String] = Map(
      TagUtils.cube -> cubeId.string,
      TagUtils.minWeight -> minWeight.value.toString,
      TagUtils.maxWeight -> maxWeight.value.toString,
      TagUtils.state -> state,
      TagUtils.revision -> revision.revisionID.toString,
      TagUtils.elementCount -> rowCount.toString)

    val writtenPath = new Path(tableID.id, s"${UUID.randomUUID()}.parquet")
    val writer: OutputWriter = factory.newInstance(
      writtenPath.toString,
      schema,
      new TaskAttemptContextImpl(
        new JobConf(serConf.value),
        new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)))

    // Write data

    it.foreach(writer.write)

    writer.close()

    // Gather file stats and output an AddFile
    val fileStatus = writtenPath
      .getFileSystem(serConf.value)
      .getFileStatus(writtenPath)

    val addFile = AddFile(
      path = writtenPath.getName,
      partitionValues = Map(),
      size = fileStatus.getLen,
      modificationTime = fileStatus.getModificationTime,
      dataChange = true,
      stats = "",
      tags = tags)

    // Add the file to the commit log and remove the old ones
    // Better to check all the metadata associated
    (Seq(addFile) ++ cubeBlocks.map(b =>
      RemoveFile(b.path, Some(System.currentTimeMillis())))).toIterator
  }

}
