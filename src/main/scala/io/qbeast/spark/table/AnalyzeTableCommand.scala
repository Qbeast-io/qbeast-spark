/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.spark.model.Revision
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.{Row, SparkSession}

/**
 * The Analyze Table command implementation
 * @param spaceRevision revision to analyze
 * @param indexedTable indexed table to analyze
 */
case class AnalyzeTableCommand(spaceRevision: Revision, indexedTable: IndexedTable)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    indexedTable.analyze(spaceRevision).map(r => Row.fromSeq(Seq(r)))

  }

}
