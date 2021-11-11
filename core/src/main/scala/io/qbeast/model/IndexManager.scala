package io.qbeast.model

import io.qbeast.IISeq

trait IndexManager[T] {

  def index(data: T, indexStatus: IndexStatus): (T, TableChanges)

  def optimize(data: T, indexStatus: IndexStatus): (T, TableChanges)

  def analyze(indexStatus: IndexStatus): IISeq[CubeId]

}
