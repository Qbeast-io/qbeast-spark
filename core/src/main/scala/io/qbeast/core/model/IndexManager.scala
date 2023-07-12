package io.qbeast.core.model

import io.qbeast.IISeq

/**
 * Index Manager template
 * @tparam DATA type of data to index
 */
trait IndexManager[DATA] {

  /**
   * Indexes given data.
   *
   * @param data the data to index
   * @param indexStatus the current index status
   * @return the changes of the index and reorganization of data
   */
  def index(data: DATA, indexStatus: IndexStatus): (DATA, TableChanges)

  /**
   * Replicates given index data.
   *
   * @param data the data to replicate
   * @param indexStatus the current index status
   * @return the changes on the index and reorganization of data
   */
  def replicate(data: DATA, indexStatus: IndexStatus): (DATA, TableChanges)

  /**
   * Analyzes the index status and returns the cubes which needs replication
   *
   * @param indexStatus the current index status
   * @return the cubes that should be replicated for improving performance
   */
  def analyze(indexStatus: IndexStatus): IISeq[CubeId]

}
