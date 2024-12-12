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
package io.qbeast.spark.index

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame

/**
 * QbeastColumns companion object.
 */
object QbeastColumns {

  /**
   * Weight column name.
   */
  val weightColumnName = "_qbeastWeight"

  /**
   * Cube column name.
   */
  val cubeColumnName = "_qbeastCube"

  /**
   * Destination file UUID column name.
   */
  val fileUUIDColumnName = "_qbeastFileUUID"

  val columnNames: Set[String] = Set(weightColumnName, cubeColumnName, fileUUIDColumnName)

  /**
   * Creates an instance for a given data frame.
   *
   * @param dataFrame
   *   the data frame
   * @return
   *   an instance
   */
  def apply(dataFrame: DataFrame): QbeastColumns = apply(dataFrame.schema)

  /**
   * Creates an instance for a given schema.
   *
   * @param schema
   *   the schema
   * @return
   *   an instance
   */
  def apply(schema: StructType): QbeastColumns = {
    val columnIndexes = schema.fieldNames.zipWithIndex.toMap
    QbeastColumns(
      weightColumnIndex = columnIndexes.getOrElse(weightColumnName, -1),
      cubeColumnIndex = columnIndexes.getOrElse(cubeColumnName, -1),
      fileUUIDColumnIndex = columnIndexes.getOrElse(fileUUIDColumnName, -1))
  }

  /**
   * Returns whether a given column is a Qbeast column.
   *
   * @param columnName
   *   the column name
   * @return
   *   the column is a Qbeast column
   */
  def contains(columnName: String): Boolean = columnNames.contains(columnName)

  /**
   * Returns whether a given field is a Qbeast column.
   *
   * @param field
   *   the field
   * @return
   *   the field is a Qbeast column
   */
  def contains(field: StructField): Boolean = contains(field.name)
}

/**
 * Qbeast columns are used to store the intermediate computation results used during indexing.
 * They are temporarily added to the original data frame and are removed when the indexing is
 * complete.
 *
 * @param weightColumnIndex
 *   the weight column index or -1 if it is missing
 * @param cubeColumnIndex
 *   the cube column index or -1 if it is missing
 * @param fileUUIDColumnIndex
 *   target file UUID column index or -1 if it is missing
 */
case class QbeastColumns(weightColumnIndex: Int, cubeColumnIndex: Int, fileUUIDColumnIndex: Int) {

  /**
   * Returns whether a given column is one of the Qbeast columns.
   *
   * @param columnIndex
   *   the column index
   * @return
   *   the column is one of the Qbeast columns
   */
  def contains(columnIndex: Int): Boolean = {
    columnIndex == weightColumnIndex ||
    columnIndex == cubeColumnIndex ||
    columnIndex == fileUUIDColumnIndex
  }

  /**
   * Returns whether the weight column exists.
   *
   * @return
   *   the weight column exists
   */
  def hasWeightColumn: Boolean = weightColumnIndex >= 0

  /**
   * Returns whether the cube column exists.
   *
   * @return
   *   the cube column exists
   */
  def hasCubeColumn: Boolean = cubeColumnIndex >= 0

  /**
   * Returns whether the destination file UUID column exists.
   *
   * @return
   *   the cube to rollup column exists
   */
  def hasFileUUIDColumn: Boolean = fileUUIDColumnIndex >= 0

}
