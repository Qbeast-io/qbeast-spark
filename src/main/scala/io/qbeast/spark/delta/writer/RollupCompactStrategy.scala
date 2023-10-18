/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model.Block
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.IndexFile
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.RowRange
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.mutable

/**
 * Implementation of CompactStrategy that uses rollup to compact small index
 * files into bigger ones containing closely related blocks of elements.
 *
 * @param tableId the table identifier
 * @param generatorFactory the generator factory
 * @param limit the desired minimum number of elements in the compacted file
 */
private[writer] class RollupCompactStrategy(
    tableId: QTableID,
    generatorFactory: IndexFileGeneratorFactory,
    desiredFileSize: Long)
    extends CompactStrategy {

  private val spark = SparkSession.active

  override def compact(indexFiles: IISeq[IndexFile]): (IISeq[IndexFile], IISeq[IndexFile]) = {
    val rollupCubeIds = computeRollupCubeIds(indexFiles)
    val generators = mutable.Map.empty[CubeId, IndexFileGenerator]
    indexFiles.foreach(copyIndexFileBlocks(rollupCubeIds, generators))
    (generators.values.map(_.close()).toIndexedSeq, indexFiles)
  }

  private def computeRollupCubeIds(indexFiles: IISeq[IndexFile]): Map[CubeId, CubeId] = {
    val rollup = new Rollup(desiredFileSize)
    indexFiles
      .flatMap(_.blocks)
      .foreach(block => rollup.populate(block.cubeId, block.elementCount))
    rollup.compute()
  }

  private def copyIndexFileBlocks(
      rollupCubeIds: Map[CubeId, CubeId],
      generators: mutable.Map[CubeId, IndexFileGenerator])(indexFile: IndexFile): Unit = {
    val rows = readIndexFile(indexFile)
    indexFile.blocks.sortBy(_.range.from).foreach {
      case Block(_, RowRange(from, to), cubeId, state, minWeight, maxWeight) =>
        val rollupCubeId = rollupCubeIds(cubeId)
        val generator =
          generators.getOrElseUpdate(rollupCubeId, generatorFactory.createIndexFileGenerator())
        generator.beginBlock(cubeId, state)
        (from until to).foreach(_ => generator.writeRow(rows.next()))
        generator.endBlock(minWeight, maxWeight)
    }
  }

  private def readIndexFile(indexFile: IndexFile): Iterator[InternalRow] = {
    val path = new Path(tableId.id, indexFile.file.path).toString()
    spark.read.parquet(path).coalesce(1).queryExecution.executedPlan.execute().toLocalIterator
  }

}
