package io.qbeast.core.model

/**
 * ColumnsToIndexSelector interface to automatically select which columns to index.
 * @tparam DATA
 *   the data to index
 */
trait ColumnsToIndexSelector[DATA] {

  /**
   * The maximum number of columns to index.
   * @return
   */
  def MAX_COLUMNS_TO_INDEX: Int

  /**
   * Selects the columns to index given a DataFrame
   * @param data
   *   the data to index
   * @return
   */
  def selectColumnsToIndex(data: DATA): Seq[String] =
    selectColumnsToIndex(data, MAX_COLUMNS_TO_INDEX)

  /**
   * Selects the columns to index with a given number of columns to index
   * @param data
   *   the data to index
   * @param numColumnsToIndex
   *   the number of columns to index
   * @return
   *   A sequence with the names of the columns to index
   */
  def selectColumnsToIndex(data: DATA, numColumnsToIndex: Int): Seq[String]

}
