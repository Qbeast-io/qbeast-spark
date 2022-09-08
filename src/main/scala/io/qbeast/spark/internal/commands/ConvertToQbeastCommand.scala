/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.commands

import org.apache.http.annotation.Experimental
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.command.{LeafRunnableCommand}

@Experimental
case class ConvertToQbeastCommand(
    path: String,
    fileFormat: String,
    columnsToIndex: Seq[String],
    cubeSize: Int = DEFAULT_CUBE_SIZE)
    extends LeafRunnableCommand {

  private val acceptedFormats = Seq("parquet", "delta")

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // TODO very basic mechanism for converting to qbeast
    val options =
      Map("columnsToIndex" -> columnsToIndex.mkString(","), "cubeSize" -> cubeSize.toString)
    if (acceptedFormats.contains(fileFormat)) {
      val df = sparkSession.read.format(fileFormat).load(path)
      df.write.format("qbeast").mode("overwrite").options(options).save(path)
    } else {
      throw new UnsupportedOperationException(s"Unsupported file format: $fileFormat")
    }

    Seq.empty

  }

}
