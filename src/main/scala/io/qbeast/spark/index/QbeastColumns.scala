/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

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
   * State column name.
   */
  val stateColumnName = "_qbeastState"

  /**
   * Revision column name.
   */
  val revisionColumnName = "_qbeastRevision"

  /**
   * Cube to replicate column name.
   */
  val cubeToReplicateColumnName = "_qbeastCubeToReplicate"

  /**
   * Cube to rollup column name.
   */
  val cubeToRollupColumnName = "_qbeastCubeToRollup"

  val columnNames = Set(
    weightColumnName,
    cubeColumnName,
    stateColumnName,
    revisionColumnName,
    cubeToReplicateColumnName,
    cubeToRollupColumnName)

  /**
   * Creates an instance for a given data frame.
   *
   * @param dataFrame the data frame
   * @return an instance
   */
  def apply(dataFrame: DataFrame): QbeastColumns = apply(dataFrame.schema)

  /**
   * Creates an instance for a given schema.
   *
   * @param schema the schema
   * @return an instance
   */
  def apply(schema: StructType): QbeastColumns = {
    val columnIndexes = schema.fieldNames.zipWithIndex.toMap
    QbeastColumns(
      weightColumnIndex = columnIndexes.getOrElse(weightColumnName, -1),
      cubeColumnIndex = columnIndexes.getOrElse(cubeColumnName, -1),
      stateColumnIndex = columnIndexes.getOrElse(stateColumnName, -1),
      revisionColumnIndex = columnIndexes.getOrElse(revisionColumnName, -1),
      cubeToReplicateColumnIndex = columnIndexes.getOrElse(cubeToReplicateColumnName, -1),
      cubeToRollupColumnIndex = columnIndexes.getOrElse(cubeToRollupColumnName, -1))
  }

  /**
   * Returns whether a given column is a Qbeast column.
   *
   * @param columnName the column name
   * @return the column is a Qbeast column
   */
  def contains(columnName: String): Boolean = columnNames.contains(columnName)

  /**
   * Returns whether a given field is a Qbeast column.
   *
   * @param field the field
   * @return the field is a Qbeast column
   */
  def contains(field: StructField): Boolean = contains(field.name)
}

/**
 * Qbeast columns are used to store the intermediate
 * computation results used during indexing. They are
 * temporarily added to the original data frame and
 * are removed when the indexing is complete.
 *
 * @param weightColumnIndex the weight column index
 *                          or -1 if it is missing
 * @param cubeColumnIndex the cube column index
 *                        or -1 if it is missing
 * @param stateColumnIndex the state column index
 *                         or -1 if it is missing
 * @param revisionColumnIndex the revision column index
 *                            or -1 if it is missing
 * @param cubeToReplicateColumnIndex the cube to replicate
 *                                   column index or -1 if
 *                                   it is missing
 * @param cubeToRollupColumnIndex the cube to rollup
 *                                   column index or -1 if
 *                                   it is missing
 */
case class QbeastColumns(
    weightColumnIndex: Int,
    cubeColumnIndex: Int,
    stateColumnIndex: Int,
    revisionColumnIndex: Int,
    cubeToReplicateColumnIndex: Int,
    cubeToRollupColumnIndex: Int) {

  /**
   * Returns whether a given column is one of the Qbeast columns.
   *
   * @param columnIndex the column index
   * @return the column is one of the Qbeast columns
   */
  def contains(columnIndex: Int): Boolean = {
    columnIndex == weightColumnIndex ||
    columnIndex == cubeColumnIndex ||
    columnIndex == stateColumnIndex ||
    columnIndex == revisionColumnIndex ||
    columnIndex == cubeToReplicateColumnIndex ||
    columnIndex == cubeToRollupColumnIndex
  }

  /**
   * Returns whether the weight column exists.
   *
   * @return the weight column exists
   */
  def hasWeightColumn: Boolean = weightColumnIndex >= 0

  /**
   * Returns whether the cube column exists.
   *
   * @return the cube column exists
   */
  def hasCubeColumn: Boolean = cubeColumnIndex >= 0

  /**
   * Returns whether the state column exists.
   *
   * @return the state column exists
   */
  def hasStateColumn: Boolean = stateColumnIndex >= 0

  /**
   * Returns whether the revision column exists.
   *
   * @return the revision column exists
   */
  def hasRevisionColumn: Boolean = revisionColumnIndex >= 0

  /**
   * Returns whether the cube to replicate column exists.
   *
   * @return the cube to replicate column exists
   */
  def hasCubeToReplicateColumn: Boolean = cubeToReplicateColumnIndex >= 0

  /**
   * Returns whether the cube to rollup column exists.
   *
   * @return the cube to rollup column exists
   */
  def hasCubeToRollupColumn: Boolean = cubeToRollupColumnIndex >= 0
}
