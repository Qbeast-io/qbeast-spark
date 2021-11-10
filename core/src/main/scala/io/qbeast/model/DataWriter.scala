package io.qbeast.model

import io.qbeast.IISeq

trait DataWriter[T <: QTableID, DATA, DataSchema, FileAction] {

  def write(
      qtable: T,
      schema: DataSchema,
      data: DATA,
      tableChanges: TableChanges): IISeq[FileAction]

}
