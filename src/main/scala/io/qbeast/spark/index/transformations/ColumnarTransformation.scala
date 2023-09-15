/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.transformations

import io.qbeast.core.transform.{LearnedStringTransformation, Transformation}

object ColumnarTransformation {

  def apply(
      transformation: Transformation,
      columnPosition: Int,
      columnSize: Int): ColumnarTransformation = transformation match {
    case t: LearnedStringTransformation =>
      LearnedStringColumnTransformation(t, columnPosition, columnSize)
    case _ => GeneralColumnTransformation(transformation, columnPosition, columnSize)
  }

}

trait ColumnarTransformation {
  val transformation: Transformation
  val columnPosition: Int
  val columnSize: Int

  def updateVector(value: Any): Unit

  def result(): Seq[Double]
}

case class GeneralColumnTransformation(
    transformation: Transformation,
    columnPosition: Int,
    columnSize: Int)
    extends ColumnarTransformation {
  private val coordinateBuilder = Seq.newBuilder[Double]
  coordinateBuilder.sizeHint(columnSize)

  override def updateVector(value: Any): Unit =
    coordinateBuilder += transformation.transform(value)

  override def result(): Seq[Double] = {
    val r = coordinateBuilder.result()
    coordinateBuilder.clear()
    r
  }

}

case class LearnedStringColumnTransformation(
    transformation: LearnedStringTransformation,
    columnPosition: Int,
    columnSize: Int)
    extends ColumnarTransformation {

  private val inputBuilder = Seq.newBuilder[Any]
  inputBuilder.sizeHint(columnSize)

  override def updateVector(value: Any): Unit =
    inputBuilder += value

  override def result(): Seq[Double] = {
    val r = transformation.transformColumn(inputBuilder.result())
    inputBuilder.clear()
    r
  }

}
