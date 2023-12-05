package io.qbeast.core.model

trait AutoIndexer[DATA] {

  def simpleName: String

  val NUM_COLUMNS_TO_INDEX: Int

  def chooseColumnsToIndex(data: DATA): Seq[String]

}
