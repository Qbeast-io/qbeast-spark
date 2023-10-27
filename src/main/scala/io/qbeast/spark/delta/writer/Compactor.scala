/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model.Block
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexFileBuilder
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.StagingUtils
import io.qbeast.core.model.TableChanges
import io.qbeast.core.model.Weight
import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.utils.State
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TaskAttemptContextImpl
import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.execution.datasources.OutputWriterFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID
import org.apache.spark.sql.delta.actions.AddFile

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
    cubeBlocks: IISeq[Block],
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
      elementCount += b.elementCount
      RemoveFile(b.file.path, Some(System.currentTimeMillis()))
    })

    val state = tableChanges.cubeState(cubeId).getOrElse(State.FLOODED)
    val revision = tableChanges.updatedRevision

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

    val addFile = if (isStaging(tableChanges.updatedRevision)) {
      AddFile(
        path = writtenPath.getName,
        partitionValues = Map.empty[String, String],
        size = fileStatus.getLen(),
        modificationTime = fileStatus.getModificationTime(),
        dataChange = false)
    } else {
      val file = new IndexFileBuilder()
        .setPath(writtenPath.getName())
        .setSize(fileStatus.getLen())
        .setModificationTime(fileStatus.getModificationTime())
        .setRevisionId(revision.revisionID)
        .beginBlock()
        .setCubeId(cubeId)
        .setMinWeight(minWeight)
        .setMaxWeight(maxWeight)
        .setElemenCount(elementCount)
        .setReplicated(state == State.ANNOUNCED || state == State.REPLICATED)
        .endBlock()
        .result()
      IndexFiles.toAddFile(false)(file)
    }
    // Add the file to the commit log and remove the old ones
    // Better to check all the metadata associated
    (Seq(addFile) ++ removedBlocks).iterator
  }

}
