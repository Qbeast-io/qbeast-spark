package io.qbeast.model

trait QueryManager[QUERY, DATA] {

  /**
   * Executes a query against a index
   * @param query the query
   * @param indexStatus the current index status
   * @return the result of the query
   */
  def query(query: QUERY, indexStatus: IndexStatus): DATA

}
