package io.qbeast.model.api

import io.qbeast.model.IndexStatus

trait QueryManager[Q, T] {

  /**
   * Executes a query against a index
   * @param query the query
   * @param indexStatus the current index status
   * @return the result of the query
   */
  def query(query: Q, indexStatus: IndexStatus): T

}
