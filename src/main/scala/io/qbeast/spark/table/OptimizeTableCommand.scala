/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.model.RevisionID
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 * The Optimize Table command implementation
 * @param revisionID the identifier of revision to optimize
 * @param indexedTable indexed table to optimize
 */
case class OptimizeTableCommand(revisionID: RevisionID, indexedTable: IndexedTable)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    indexedTable.optimize(revisionID)
    Seq.empty[Row]
  }

}
