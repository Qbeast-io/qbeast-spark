/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.core.model.CubeId
import org.apache.spark.sql.catalyst.InternalRow
import io.qbeast.core.model.Block
import io.qbeast.core.model.File
import io.qbeast.core.model.RowRange
import io.qbeast.core.model.Weight
import io.qbeast.spark.index.QbeastColumns

/**
 * Builder for creating Block instances while writing an index file.
 *
 * @param cubeId the cube identifier
 * @param from the position of the first block element in the file
 * @param state the cube state
 * @param maxWeight the maximum element weight in the block
 * @param qbeastColumns the Qbeast-specific columns
 */
private[writer] class BlockBuilder(
    val cubeId: CubeId,
    from: Long,
    state: String,
    maxWeight: Weight,
    qbeastColumns: QbeastColumns) {

  private var file: Option[File] = None
  private var to = from
  private var minWeight = Weight.MaxValue

  /**
   * Updates the state after a given extended row containing the Qbeast-specific
   * fileds was written
   *
   * @param exrendedRow the extended row with Qbeast-specific fields.
   * @return return this instance
   */
  def rowWritten(extendedRow: InternalRow): BlockBuilder = {
    val weight = getWeight(extendedRow)
    minWeight = Weight.min(minWeight, weight)
    to += 1
    this
  }

  /**
   * Updates the state after the index file was written.
   *
   * @param file the written index file
   * @return return this instance
   */
  def fileWritten(file: File): BlockBuilder = {
    this.file = Some(file)
    this
  }

  /**
   * Builds the block.
   *
   * @return return this instance
   */
  def result(): Block = {
    require(file.isDefined)
    Block(file.get, RowRange(from, to), cubeId, state, minWeight, maxWeight)
  }

  private def getWeight(extendedRow: InternalRow): Weight = {
    Weight(extendedRow.getInt(qbeastColumns.weightColumnIndex))
  }

}
