/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 * The Optimize Table command implementation
 * @param revisionTimestamp space to optimize
 * @param indexedTable indexed table to optimize
 */
case class OptimizeTableCommand(revisionTimestamp: Long, indexedTable: IndexedTable)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    indexedTable.optimize(revisionTimestamp)
    Seq.empty[Row]
  }

}
