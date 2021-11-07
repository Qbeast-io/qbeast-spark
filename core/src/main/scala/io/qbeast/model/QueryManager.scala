package io.qbeast.model

trait QueryManager[Q, T] {

  def query(query: Q, indexStatus: IndexStatus): T

}
