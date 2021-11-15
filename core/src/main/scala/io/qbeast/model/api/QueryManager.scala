package io.qbeast.model.api

import io.qbeast.model.IndexStatus

trait QueryManager[Q, T] {

  def query(query: Q, indexStatus: IndexStatus): T

}
