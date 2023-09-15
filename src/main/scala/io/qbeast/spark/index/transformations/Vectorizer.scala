/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.transformations

import io.qbeast.core.model._
import io.qbeast.spark.index.QbeastColumns.cubeToReplicateColumnName
import org.apache.spark.sql.Row

import scala.collection.mutable

class Vectorizer(
    revision: Revision,
    isReplication: Boolean,
    weightIndex: Int,
    columnIndices: IndexedSeq[Int],
    bufferCapacity: Int) {

  private val columnarTransformations: IndexedSeq[ColumnarTransformation] =
    revision.transformations
      .zip(columnIndices)
      .map { case (t, i) => ColumnarTransformation(t, i, bufferCapacity) }
      .toIndexedSeq

  private val rowWeightsBuilder: mutable.Builder[Weight, Seq[Weight]] = Seq.newBuilder[Weight]
  rowWeightsBuilder.sizeHint(bufferCapacity)

  private val bytesBuilder: mutable.Builder[Array[Byte], Seq[Array[Byte]]] =
    Seq.newBuilder[Array[Byte]]

  bytesBuilder.sizeHint(if (isReplication) bufferCapacity else 0)
  private var rowCount: Int = 0

  def update(row: Row): Unit = {
    rowWeightsBuilder += Weight(row.getAs[Int](weightIndex))
    if (isReplication) bytesBuilder += row.getAs[Array[Byte]](cubeToReplicateColumnName)
    columnarTransformations.foreach(t => t.updateVector(row(t.columnPosition)))
    rowCount += 1
  }

  private def getWeights: Seq[Weight] = {
    val r = rowWeightsBuilder.result()
    rowWeightsBuilder.clear()
    r
  }

  private def getCubeIds: Seq[Option[CubeId]] = {
    val r =
      if (isReplication) bytesBuilder.result().map(b => Some(revision.createCubeId(b)))
      else Seq.fill(rowCount)(None)
    bytesBuilder.clear()
    r
  }

  private def getCoordinates: IndexedSeq[IndexedSeq[Double]] =
    columnarTransformations.map(t => t.result()).transpose

  def vectorizedIndexing(
      rows: Iterator[Row],
      pointWeightIndexer: PointWeightIndexer): Iterator[Row] = {
    val (rowsForPoints, originalRows) = rows.duplicate
    val newRowsBuilder = Seq.newBuilder[Row]
    val getNewRows = () => {
      val cubeIds =
        result().map { case (point, weight, parentOpt) =>
          pointWeightIndexer.findTargetCubeIds(point, weight, parentOpt)
        }.toIterator
      cubeIds
        .zip(originalRows)
        .flatMap { case (cubes, row) =>
          cubes.map(c => Row(row.toSeq :+ c.bytes: _*))
        }
    }

    rowsForPoints.foreach { row =>
      update(row)
      if (rowCount == bufferCapacity) newRowsBuilder ++= getNewRows()
    }
    newRowsBuilder ++= getNewRows()
    newRowsBuilder.result().toIterator
  }

  def getPartitionCubeMetadata(
      rows: Iterator[Row],
      cubeWeightsBuilder: CubeWeightsBuilder): Iterator[CubeDomain] = {
    val updateCubeWeightBuilder = () => {
      result().foreach { case (point, weight, parentOpt) =>
        cubeWeightsBuilder.update(point, weight, parentOpt)
      }
    }

    rows.foreach { row =>
      update(row)
      if (rowCount == bufferCapacity) {
        updateCubeWeightBuilder()
      }
    }
    updateCubeWeightBuilder()
    cubeWeightsBuilder.result().toIterator
  }

  def result(): Seq[(Point, Weight, Option[CubeId])] = {
    if (rowCount == 0) Nil
    else {
      val coordinates = getCoordinates
      val rowWeights = getWeights
      val parentCubeOpts = getCubeIds

      rowCount = 0
      (coordinates, rowWeights, parentCubeOpts).zipped.toSeq.map {
        case (coords, weight, cubeOpt) =>
          (Point(coords: _*), weight, cubeOpt)
      }
    }
  }

}
