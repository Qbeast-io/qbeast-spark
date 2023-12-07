package io.qbeast.core.model

/**
 * AutoIndexer interface to automatically choose which columns to index.
 * @tparam DATA
 *   the data to index
 */
trait AutoIndexer[DATA] {

  /**
   * The maximum number of columns to index.
   * @return
   */
  def MAX_COLUMNS_TO_INDEX: Int

  /**
   * Chooses the columns to index.
   * @param data
   *   the data to index
   * @param numColumnsToIndex
   *   the number of columns to index if not specified, the default value is
   *   MAX_NUM_COLUMNS_TO_INDEX
   * @return
   *   A sequence with the names of the columns to index
   */
  def chooseColumnsToIndex(data: DATA, numColumnsToIndex: Int = MAX_COLUMNS_TO_INDEX): Seq[String]

}
