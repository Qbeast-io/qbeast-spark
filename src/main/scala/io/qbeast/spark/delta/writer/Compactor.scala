/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model.{CubeId, QTableID, QbeastBlock, StagingUtils, TableChanges, Weight}
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

/**
 * Compacts the information from a set of CubeBlocks into a single block
 * @param tableID the table identifier
 * @param factory output writer factory
 * @param serConf configuration to serialize the data
 * @param schema the schema
 * @param cubeId the cubeId to compact
 * @param cubeBlocks the blocks to compact
 * @param tableChanges the TableChanges
 */

case class Compactor(
    tableID: QTableID,
    factory: OutputWriterFactory,
    serConf: SerializableConfiguration,
    schema: StructType,
    cubeId: CubeId,
    cubeBlocks: IISeq[QbeastBlock],
    tableChanges: TableChanges)
    extends Serializable
    with StagingUtils {

  def writeBlock(it: Iterator[InternalRow]): Iterator[FileAction] = {

    var minWeight = Weight.MaxValue
    var maxWeight = Weight.MaxValue
    var elementCount = 0L

    // Use one single map to compute metadata
    // (min, max weight and elementCount)
    val removedBlocks = cubeBlocks.map(b => {
      // minWeight it's computed as the minimum of the minWeights
      if (b.minWeight < minWeight) minWeight = b.minWeight
      // maxWeight it's computed as the minimum of the maxWeights
      if (b.maxWeight < maxWeight) maxWeight = b.maxWeight
      elementCount = elementCount + b.elementCount
      RemoveFile(b.path, Some(System.currentTimeMillis()))
    })

    val state = tableChanges.cubeState(cubeId).getOrElse(State.FLOODED)
    val revision = tableChanges.updatedRevision

    // Update the tags of the block with the information of the cubeBlocks
    val tags: Map[String, String] =
      if (isStaging(revision)) null
      else {
        Map(
          TagUtils.cube -> cubeId.string,
          TagUtils.minWeight -> minWeight.value.toString,
          TagUtils.maxWeight -> maxWeight.value.toString,
          TagUtils.state -> state,
          TagUtils.revision -> revision.revisionID.toString,
          TagUtils.elementCount -> elementCount.toString)
      }

    val writtenPath = new Path(tableID.id, s"${UUID.randomUUID()}.parquet")
    val writer: OutputWriter = factory.newInstance(
      writtenPath.toString,
      schema,
      new TaskAttemptContextImpl(
        new JobConf(serConf.value),
        new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)))

    // Write data

    try {
      it.foreach(writer.write)
    } finally {
      writer.close()
    }

    // Gather file stats and output an AddFile
    val fileStatus = writtenPath
      .getFileSystem(serConf.value)
      .getFileStatus(writtenPath)

    val addFile = AddFile(
      path = writtenPath.getName,
      partitionValues = Map(),
      size = fileStatus.getLen,
      modificationTime = fileStatus.getModificationTime,
      dataChange = false,
      stats = "",
      tags = tags)

    // Add the file to the commit log and remove the old ones
    // Better to check all the metadata associated
    (Seq(addFile) ++ removedBlocks).toIterator
  }

}
