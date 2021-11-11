package io.qbeast.model

import io.qbeast.IISeq

trait DataWriter[DATA, DataSchema, FileAction] {

  def write(
      qtable: QTableID,
      schema: DataSchema,
      data: DATA,
      tableChanges: TableChanges): IISeq[FileAction]

}
