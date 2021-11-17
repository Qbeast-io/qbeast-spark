package io.qbeast.model.api

import io.qbeast.IISeq
import io.qbeast.model.{CubeId, IndexStatus, TableChanges}

trait IndexManager[T] {

  /**
   * Indexes the data
   * @param data the data
   * @param indexStatus the current index status
   * @return the changes of the index
   */
  def index(data: T, indexStatus: IndexStatus): (T, TableChanges)

  /**
   * Optimizes the index
   * @param data the data
   * @param indexStatus the current index status
   * @return the changes on the index
   */
  def optimize(data: T, indexStatus: IndexStatus): (T, TableChanges)

  /**
   * Analyzes the current index status
   * @param indexStatus the current index status
   * @return the sequence of cubes that should be optimized for improving performance
   */
  def analyze(indexStatus: IndexStatus): IISeq[CubeId]

}
