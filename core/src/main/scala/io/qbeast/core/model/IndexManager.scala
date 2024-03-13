package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Index Manager template
 * @tparam DATA
 *   type of data to index
 */
trait IndexManager[DATA] {

  /**
   * Indexes the data
   * @param data
   *   the data to index
   * @param indexStatus
   *   the current index status
   * @return
   *   the changes of the index and reorganization of data
   */
  def index(data: DATA, indexStatus: IndexStatus): (DATA, TableChanges)

  /**
   * Optimizes the index
   * @param data
   *   the data of the index
   * @param indexStatus
   *   the current index status
   * @return
   *   the changes on the index and reorganization of data
   */
  def optimize(data: DATA, indexStatus: IndexStatus): (DATA, TableChanges)

  /**
   * Analyzes the current index status
   * @param indexStatus
   *   the current index status
   * @return
   *   the sequence of cubes that should be optimized for improving performance
   */
  def analyze(indexStatus: IndexStatus): IISeq[CubeId]

}
