package io.qbeast.core.model

trait AutoIndexer[DATA] {

  val MAX_COLUMNS_TO_INDEX: Int

  def chooseColumnsToIndex(data: DATA): Seq[String]

}
