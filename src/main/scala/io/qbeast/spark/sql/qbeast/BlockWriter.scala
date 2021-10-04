/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.qbeast

import io.qbeast.spark.index.{ColumnsToIndex, CubeId, QbeastColumns, Weight}
import io.qbeast.spark.model.SpaceRevision
import io.qbeast.spark.sql.utils.TagUtils.{
  cubeTag,
  elementCountTag,
  indexedColsTag,
  spaceTag,
  stateTag,
  weightMaxTag,
  weightMinTag
}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{JobConf, TaskAttemptContextImpl, TaskAttemptID}
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import java.util.UUID

/**
 * BlockWriter is in charge of writing the qbeast data into files
 * @param dataPath path of the table
 * @param schema schema of the original data
 * @param schemaIndex schema with qbeast metadata columns
 * @param factory output writer factory
 * @param serConf configuration to serialize the data
 * @param qbeastColumns qbeast metadata columns
 * @param columnsToIndex columns of the original data that are used for indexing
 * @param spaceRevision space revision of the data to write
 */
case class BlockWriter(
    dataPath: Path,
    schema: StructType,
    schemaIndex: StructType,
    factory: OutputWriterFactory,
    serConf: SerializableConfiguration,
    qbeastColumns: QbeastColumns,
    columnsToIndex: Seq[String],
    spaceRevision: SpaceRevision,
    weightMap: Map[CubeId, Weight])
    extends Serializable {
  private val dimensionCount = columnsToIndex.length

  /**
   * Writes rows in corresponding files
   * @param iter iterator of rows
   * @return the sequence of files added
   */
  def writeRow(iter: Iterator[InternalRow]): Iterator[AddFile] = {
    if (!iter.hasNext) {
      return Iterator.empty
    }

    iter
      .foldLeft[Map[CubeId, BlockContext]](Map()) { case (blocks, row) =>
        val cubeId = CubeId(dimensionCount, row.getBinary(qbeastColumns.cubeColumnIndex))
        val state = row.getString(qbeastColumns.stateColumnIndex)
        // TODO make sure this does not compromise the structure of the index
        // It could happen than estimated weights
        // doesn't include all the cubes present in the final indexed dataframe
        // we save those newly added leaves with the max weight possible
        val maxWeight = weightMap.getOrElse(cubeId, Weight.MaxValue)
        val blockCtx =
          blocks.getOrElse(cubeId, buildWriter(cubeId, state, maxWeight))

        // The row with only the original columns
        val cleanRow = Seq.newBuilder[Any]
        for (i <- (0 until row.numFields)) {
          if (!qbeastColumns.contains(i)) {
            cleanRow += row.get(i, schemaIndex(i).dataType)
          }
        }
        // Writing the data in a single file.
        blockCtx.writer.write(InternalRow.fromSeq(cleanRow.result()))
        blocks.updated(cubeId, blockCtx.update())
      }
      .values
      .flatMap {
        case BlockContext(blockStats, _, _) if blockStats.rowCount == 0 =>
          Iterator.empty // Do nothing, this  is a empty partition
        case BlockContext(
              BlockStats(cube, maxWeight, minWeight, state, rowCount),
              writer,
              path) =>
          val tags = Map(
            cubeTag -> cube,
            weightMinTag -> minWeight.value.toString,
            weightMaxTag -> maxWeight.value.toString,
            stateTag -> state,
            spaceTag -> JsonUtils.toJson(spaceRevision),
            indexedColsTag -> ColumnsToIndex.encode(columnsToIndex),
            elementCountTag -> rowCount.toString)

          writer.close()

          val fileStatus = path
            .getFileSystem(serConf.value)
            .getFileStatus(path)

          Iterator(
            AddFile(
              path = path.getName(),
              partitionValues = Map(),
              size = fileStatus.getLen,
              modificationTime = fileStatus.getModificationTime,
              dataChange = true,
              stats = "",
              tags = tags))

      }
  }.toIterator

  /*
   * Creates the context to write a new cube in a new file and collect stats
   * @param cubeId a cube identifier
   * @param state the status of cube
   * @return
   */
  private def buildWriter(cubeId: CubeId, state: String, maxWeight: Weight): BlockContext = {
    val writtenPath = new Path(dataPath, s"${UUID.randomUUID()}.parquet")
    val writer: OutputWriter = factory.newInstance(
      writtenPath.toString,
      schema,
      new TaskAttemptContextImpl(
        new JobConf(serConf.value),
        new TaskAttemptID("", 0, TaskType.REDUCE, 0, 0)))
    BlockContext(BlockStats(cubeId.string, state, maxWeight), writer, writtenPath)
  }

  /*
   * Container class that keeps all the mutable information we need to update a
   * block when iterating over a partition.
   * @param stats the current version of the block's stats
   * @param writer an instance of the file writer
   * @param path the path of the written file
   */
  private case class BlockContext(stats: BlockStats, writer: OutputWriter, path: Path) {
    def update(): BlockContext = this.copy(stats = stats.update())
  }

}
