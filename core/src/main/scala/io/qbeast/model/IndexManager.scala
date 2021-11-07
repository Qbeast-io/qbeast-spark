package io.qbeast.model

trait IndexManager[T] {

  def index(data: T, indexStatus: IndexStatus): TableChanges

  def optimize(data: T, indexStatus: IndexStatus): TableChanges
}
