/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.model

import scala.collection.immutable.IndexedSeq

/**
 * Represents the configuration and space where the data is fitted.
 *
 * @param id the identifier of the revision
 * @param timestamp moment when the revision was created in milliseconds
 * @param desiredCubeSize the desired size of the cubes for the revision
 * @param indexedColumns the columns indexed on the revision
 * @param transformations transformations for each coordinate in the space
 */
case class Revision(
    id: RevisionID,
    timestamp: Long,
    desiredCubeSize: Int,
    indexedColumns: Seq[String],
    transformations: IndexedSeq[Transformation]) {

  val dimensionCount = indexedColumns.length

  def root: CubeId = CubeId.root(dimensionCount)

  /**
   * Transforms given coordinates.
   *
   * @param coordinates the coordinates
   * @return the point with transformed coordinates
   */
  def transform(coordinates: IndexedSeq[Double]): Point = {
    Point(transformations.zip(coordinates).map { case (trans, value) =>
      trans.transform(value)
    })
  }

  /**
   * Transforms a given point.
   *
   * @param point the point
   * @return the point with transformed coordinates
   */
  def transform(point: Point): Point = transform(point.coordinates)

  /**
   * Returns whether a given data frame fits the revision for each index column.
   *
   * @param dataFrame the data frame
   * @return the data frame fits the space revision
   *
   *  def contains(dataFrame: DataFrame): Boolean = {
   *    transformations
   *      .zip(ColumnInfo.get(dataFrame, indexedColumns))
   *      .forall { case (transformation, info) =>
   *        transformation.min <= info.min && info.max <= transformation.max
   *      }
   *  }
   */
}

/**
 * Companion object for Revision
 * Creates a new Revision on a given Dataframe,
 * columns to index and desired cube size
 */
object Revision {

  /**
   * Creates a revision from a given data frame to index.
   *
   * @param dataFrame the data frame
   * @param columnsToIndex the columns to index
   * @param desiredCubeSize the desired cube size
   * @return a revision
   *
   *  def apply(dataFrame: DataFrame, columnsToIndex: Seq[String], desiredCubeSize: Int): Revision = {
   *    val transformations = ColumnInfo
   *      .get(dataFrame, columnsToIndex)
   *      .map(info => {
   *        val expansion = (info.max - info.min) / 2
   *        LinearTransformation(info.min - expansion, info.max + expansion)
   *      })
   *      .toIndexedSeq
   *
   *    val timestamp = System.currentTimeMillis()
   *    new Revision(
   *      timestamp,
   *      timestamp,
   *      desiredCubeSize = desiredCubeSize,
   *      indexedColumns = columnsToIndex,
   *      transformations = transformations)
   *  }
   */
}
