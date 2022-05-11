/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import jdk.jfr.Experimental
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.RunnableCommand

@Experimental
case class PartiallyConvertToQbeastCommand(
    path: String,
    fileFormat: String,
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {

    fileFormat match {
      case "delta" => //
      case "parquet" => //
      case _ => throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    }
    // TODO
    Seq.empty

  }

}
