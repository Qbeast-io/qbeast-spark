/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.delta

import io.qbeast.core.model.{CubeId, QTableID, Revision}
import io.qbeast.spark.utils.{State, TagColumns}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

/**
 * Loads cube data from a specific table
 * @param tableID the table identifier
 */
case class CubeDataLoader(tableID: QTableID) {

  private val spark = SparkSession.active

  private val snapshot = DeltaLog.forTable(SparkSession.active, tableID.id).unsafeVolatileSnapshot

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

}
