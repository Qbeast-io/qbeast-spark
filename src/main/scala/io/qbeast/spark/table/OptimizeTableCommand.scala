/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.spark.model.SpaceRevision
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 * The Optimize Table command implementation
 * @param spaceRevision space to optimize
 * @param indexedTable indexed table to optimize
 */
case class OptimizeTableCommand(spaceRevision: SpaceRevision, indexedTable: IndexedTable)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    indexedTable.optimize(spaceRevision)
    Seq.empty[Row]
  }

}
