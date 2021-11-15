package io.qbeast.model.api

import io.qbeast.IISeq
import io.qbeast.model.{QTableID, TableChanges}

trait DataWriter[DATA, DataSchema, FileAction] {

  def write(
      qtable: QTableID,
      schema: DataSchema,
      data: DATA,
      tableChanges: TableChanges): IISeq[FileAction]

}
