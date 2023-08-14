/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Builder for creating QueryFile instances.
 *
 * @param file the file
 */
class QueryFileBuilder(file: File) {
  require(file != null)

  private val ranges = Seq.newBuilder[RowRange]

  /**
   * Ads a given range.
   *
   * @param range the range to add
   * @return this instance
   */
  def addRange(range: RowRange): QueryFileBuilder = {
    ranges += range
    this
  }

  /**
   * Adds the range of a given block. This method does not check that the block
   * references the same file.
   *
   * @param block the block owning the range  to add
   * @return this instance
   */
  def addBlock(block: Block): QueryFileBuilder = {
    addRange(block.range)
  }

  /**
   * Creates the QueryFile instance.
   */
  def result(): QueryFile = {
    QueryFile(file, ranges.result())
  }

}
