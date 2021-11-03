/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.model

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.types.DoubleType
import io.qbeast.model.Revision
import io.qbeast.model.LinearTransformation

/**
 * Column information.
 * @param column name of the column
 * @param min min value of the column
 * @param max max value of the column
 */
case class ColumnInfo(column: String, min: Double, max: Double)

object ColumnInfo {

  /**
   * Returns the column information for the specified columns of a given data frame.
   *
   * @param dataFrame the data frame
   * @param columns   the columns of interest
   * @return the column information
   */
  def get(dataFrame: DataFrame, columns: Seq[String]): Seq[ColumnInfo] = {
    val first :: rest = columns
      .flatMap(column => Seq(min(column), max(column)))
      .map(_.cast(DoubleType))
      .toList
    val aggregates = dataFrame
      .agg(first, rest: _*)
      .first()
    for (i <- columns.indices)
      yield ColumnInfo(
        columns(i),
        aggregates.getAs[Double](2 * i),
        aggregates.getAs[Double](2 * i + 1))
  }

}

object RevisionUtil {

  def createRevisionFromDF(
      dataFrame: DataFrame,
      columnsToIndex: Seq[String],
      desiredCubeSize: Int): Revision = {
    val transformations = ColumnInfo
      .get(dataFrame, columnsToIndex)
      .map(info => {
        val expansion = (info.max - info.min) / 2
        LinearTransformation(info.min - expansion, info.max + expansion)
      })
      .toIndexedSeq

    val timestamp = System.currentTimeMillis()
    new Revision(
      timestamp,
      timestamp,
      desiredCubeSize = desiredCubeSize,
      indexedColumns = columnsToIndex,
      transformations = transformations)
  }

  def revisionContains(
      revision: Revision,
      dataFrame: DataFrame,
      indexedColumns: Seq[String]): Boolean = {
    revision.transformations
      .zip(ColumnInfo.get(dataFrame, indexedColumns))
      .forall {
        case (transformation: LinearTransformation, info) =>
          transformation.min <= info.min && info.max <= transformation.max
        case _ => false
      }

  }

}
