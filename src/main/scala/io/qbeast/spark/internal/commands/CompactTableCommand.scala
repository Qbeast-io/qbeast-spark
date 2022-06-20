/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import io.qbeast.core.model.RevisionID
import io.qbeast.spark.table.IndexedTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand

case class CompactTableCommand(revisionID: RevisionID, indexedTable: IndexedTable)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    indexedTable.compact(revisionID)
    Seq.empty
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    SparkSession.active.emptyDataFrame.queryExecution.logical

}
