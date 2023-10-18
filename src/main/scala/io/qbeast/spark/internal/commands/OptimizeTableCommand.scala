/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model.RevisionID
import io.qbeast.spark.table.IndexedTable
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 * The Optimize Table command implementation
 *
 * @param revisionID the identifier of revision to optimize
 * @param indexedTable indexed table to optimize
 */
@deprecated("Moved to a different service", "0.5")
case class OptimizeTableCommand(revisionID: RevisionID, indexedTable: IndexedTable)
    extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    indexedTable.optimize(revisionID)
    Seq.empty[Row]
  }

}
