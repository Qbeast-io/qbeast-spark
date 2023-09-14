/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.transformations

import breeze.linalg.DenseMatrix
import io.qbeast.core.model.{
  CubeDomain,
  CubeId,
  CubeWeightsBuilder,
  Point,
  PointWeightIndexer,
  Revision,
  Weight
}
import io.qbeast.spark.index.QbeastColumns.cubeToReplicateColumnName
import org.apache.spark.sql.Row

import java.util.concurrent.TimeUnit.NANOSECONDS
import scala.collection.mutable

class Vectorizer(
    revision: Revision,
    isReplication: Boolean,
    weightIndex: Int,
    columnIndices: Seq[Int],
    bufferCapacity: Int) {

  private val vectorTransformations: Seq[VectorTransformation] =
    revision.transformations
      .zip(columnIndices)
      .map { case (t, i) =>
        VectorTransformation(t, i, bufferCapacity)
      }

  private val rowWeightsBuilder: mutable.Builder[Weight, Seq[Weight]] = Seq.newBuilder[Weight]
  rowWeightsBuilder.sizeHint(bufferCapacity)

  private val bytesBuilder: mutable.Builder[Array[Byte], Seq[Array[Byte]]] =
    Seq.newBuilder[Array[Byte]]

  bytesBuilder.sizeHint(if (isReplication) bufferCapacity else 0)
  private var rowCount: Int = 0

  def isFull: Boolean = rowCount == bufferCapacity

  def update(row: Row): Unit = {
    rowWeightsBuilder += Weight(row.getAs[Int](weightIndex))
    if (isReplication) bytesBuilder += row.getAs[Array[Byte]](cubeToReplicateColumnName)
    vectorTransformations.foreach(t => t.updateVector(rowCount, row(t.columnPosition)))
    rowCount += 1
  }

  private def getWeights: Seq[Weight] = rowWeightsBuilder.result()

  private def getCubeBytes: Seq[Option[CubeId]] = {
    if (isReplication) bytesBuilder.result().map(b => Some(revision.createCubeId(b)))
    else Seq.fill(rowCount)(None)
  }

  private def getPoints: Seq[Point] = {
    val transformedColumns = vectorTransformations.map(t => t.vectorTransform(rowCount))
    val colsAsCoords = DenseMatrix(transformedColumns: _*)
    (0 until rowCount).map { i => Point(colsAsCoords(::, i).toScalaVector: _*) }
  }

  private def reset(): Unit = {
    rowWeightsBuilder.clear()
    bytesBuilder.clear()
    vectorTransformations.foreach(_.clear())
    rowCount = 0
  }

  def vectorizedIndexing(
      rows: Iterator[Row],
      pointWeightIndexer: PointWeightIndexer): Iterator[Row] = {
    val (rowsForPoints, originalRows) = rows.duplicate
    val newRowsBuilder = Seq.newBuilder[Row]

    rowsForPoints.foreach { row =>
      update(row)
      if (isFull) {
        val newRows = result()
          .map { case (point, weight, parentOpt) =>
            pointWeightIndexer.findTargetCubeIds(point, weight, parentOpt)
          }
          .toIterator
          .zip(originalRows)
          .flatMap { case (cubes, row) =>
            cubes.map(c => Row(row.toSeq :+ c.bytes: _*))
          }

        newRowsBuilder ++= newRows
      }
    }
    val newRows = result()
      .map { case (point, weight, parentOpt) =>
        pointWeightIndexer.findTargetCubeIds(point, weight, parentOpt)
      }
      .toIterator
      .zip(originalRows)
      .flatMap { case (cubes, row) =>
        cubes.map(c => Row(row.toSeq :+ c.bytes: _*))
      }

    newRowsBuilder ++= newRows
    newRowsBuilder.result().toIterator
  }

  def getPartitionCubeMetadata(
      rows: Iterator[Row],
      cubeWeightsBuilder: CubeWeightsBuilder): Iterator[CubeDomain] = {
    rows.foreach { row =>
      update(row)
      if (isFull) {
        result().foreach { case (point, weight, parentOpt) =>
          cubeWeightsBuilder.update(point, weight, parentOpt)
        }
      }
    }
    result().foreach { case (point, weight, parentOpt) =>
      cubeWeightsBuilder.update(point, weight, parentOpt)
    }
    cubeWeightsBuilder.result().toIterator
  }

  def result(): Seq[(Point, Weight, Option[CubeId])] = {
    if (rowCount == 0) Nil
    else {
      val points = getPoints
      val rowWeights = getWeights
      val parentCubeOpts = getCubeBytes
      val results = (points, rowWeights, parentCubeOpts).zipped.toSeq

      reset()
      results
    }
  }

}
