/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.model.{CubeId, QTableID, Revision}
import io.qbeast.spark.index.QbeastColumns.cubeToReplicateColumnName
import io.qbeast.spark.utils.{State, TagUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

case class DataLoader(qtable: QTableID) {

  private val spark = SparkSession.active

  private val snapshot = DeltaLog.forTable(SparkSession.active, qtable.id).snapshot

  def loadSetWithCubeColumn(cubeSet: Set[CubeId], revision: Revision): DataFrame = {
    cubeSet
      .map(cube => {
        loadWithCubeColumn(cube, revision)
      })
      .filter(df => !df.isEmpty)
      .reduce(_ union _)
  }

  def loadWithCubeColumn(cubeId: CubeId, revision: Revision): DataFrame = {

    val df = load(cubeId, revision)
    if (df.isEmpty) spark.emptyDataFrame
    else df.withColumn(cubeToReplicateColumnName, lit(cubeId.bytes))
  }

  def load(cube: CubeId, revision: Revision): DataFrame = {

    val cubeBlocks = snapshot.allFiles
      .filter(file =>
        file.tags(TagUtils.revision) == revision.revisionID.toString &&
          cube.string == file.tags(TagUtils.cube) &&
          file.tags(TagUtils.state) != State.ANNOUNCED)
      .collect()

    val fileNames = cubeBlocks.map(f => new Path(qtable.id, f.path).toString)
    spark.read
      .format("parquet")
      .load(fileNames: _*)

  }

}
