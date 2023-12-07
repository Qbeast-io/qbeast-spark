package io.qbeast.core.model

/**
 * AutoIndexer interface to automatically choose which columns to index.
 * @tparam DATA
 *   the data to index
 */
trait AutoIndexer[DATA] {

  /**
   * Chooses the columns to index.
   * @param data
   *   the data to index
   * @return
   *   A sequence with the names of the columns to index
   */
  def chooseColumnsToIndex(data: DATA): Seq[String]

  /**
   * Chooses the columns to index.
   * @param data
   *   the data to index
   * @param numColumnsToIndex
   *   the number of columns to index
   * @return
   *   A sequence with the names of the columns to index
   */
  def chooseColumnsToIndex(data: DATA, numColumnsToIndex: Int): Seq[String]

}
