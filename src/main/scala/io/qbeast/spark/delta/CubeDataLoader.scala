/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.{CubeId, QTableID, Revision, RevisionID}
import io.qbeast.spark.utils.TagColumns.{elementCount, revision}
import io.qbeast.spark.utils.{State, TagColumns, TagUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, sum}

/**
 * Loads cube data from a specific table
 * @param tableID the table identifier
 */
case class CubeDataLoader(tableID: QTableID) {

  private val spark = SparkSession.active

  private val snapshot = DeltaLog.forTable(SparkSession.active, tableID.id).snapshot

  /**
   * Loads the data from a set of cubes in a specific revision
   * and adds column information
   * @param cubeSet the set of cubes to load
   * @param revision the revision to load
   * @param columnName the column name to add
   * @return the dataframe
   */
  def loadSetWithCubeColumn(
      cubeSet: Set[CubeId],
      revision: Revision,
      columnName: String): DataFrame = {
    cubeSet
      .map(loadWithCubeColumn(_, revision, columnName))
      .filter(df => !df.isEmpty)
      .reduceOption(_ union _)
      .getOrElse(spark.emptyDataFrame)
  }

  /**
   * Loads the data from a single cube in a specific revision
   * and adds column information
   * @param cubeId the cube to load
   * @param revision the revision to load
   * @param columnName the column name to add
   * @return the dataframe
   */
  def loadWithCubeColumn(cubeId: CubeId, revision: Revision, columnName: String): DataFrame = {
    loadCubeData(cubeId, revision).withColumn(columnName, lit(cubeId.bytes))
  }

  /**
   * Loads the data from a single cube in a specific revision
   * @param cube the cube to load
   * @param revision the revision to load
   * @return the dataframe without extra information
   */

  def loadCubeData(cube: CubeId, revision: Revision): DataFrame = {

    val cubeBlocks = snapshot.allFiles
      .where(
        TagColumns.revision === lit(revision.revisionID.toString) &&
          TagColumns.cube === lit(cube.string) &&
          TagColumns.state != lit(State.ANNOUNCED))
      .collect()

    val fileNames = cubeBlocks.map(f => new Path(tableID.id, f.path).toString)
    spark.read
      .format("parquet")
      .load(fileNames: _*)

  }

  def loadFilesFromRevisions(revisions: Seq[Revision]): Seq[AddFile] = {
    val allFiles = snapshot.allFiles
    val revisionIdStrings = revisions.map(_.revisionID)
    allFiles
      .where(
        s"${TagUtils.revision} IN (${revisionIdStrings.mkString(",")}) AND ${TagUtils.state} === ${State.FLOODED}")
      .collect()
  }

  def revisionRowCount(): Map[RevisionID, Long] = {
    import spark.implicits._

    snapshot.allFiles
      .where("tags IS NOT NULL AND tags.state == 'FLOODED'")
      .groupBy(revision.cast("long"))
      .agg(sum(elementCount).cast("long"))
      .as[(Long, Long)]
      .collect()
      .toMap

  }

}
