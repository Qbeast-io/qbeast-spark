package io.qbeast.model.api

import io.qbeast.IISeq
import io.qbeast.model.{CubeId, IndexStatus, TableChanges}

trait IndexManager[T] {

  def index(data: T, indexStatus: IndexStatus): (T, TableChanges)

  def optimize(data: T, indexStatus: IndexStatus): (T, TableChanges)

  def analyze(indexStatus: IndexStatus): IISeq[CubeId]

}
