/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.transformations

import breeze.linalg.DenseVector
import io.qbeast.core.transform.{LinearTransformation, Transformation}

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.time.Instant

class VectorizedLinearTransformation(
    override val transformation: LinearTransformation,
    override val columnPosition: Int,
    override val columnSize: Int)
    extends VectorTransformation {

  import transformation.orderedDataType.ordering._

  private val mn: Double = transformation.minNumber.toDouble

  private val scale: Double = {
    val mx = transformation.maxNumber.toDouble
    require(mx > mn, "Range cannot be not null, and max must be > min")
    1.0 / (mx - mn)
  }

  private var vector: DenseVector[Double] = DenseVector.zeros[Double](columnSize)

  override def updateVector(i: Int, value: Any): Unit = {
    val num = if (value == null) transformation.nullValue else value
    vector(i) = num match {
      case v: Double => v
      case v: Float => v.toDouble
      case v: Long => v.toDouble
      case v: Int => v.toDouble
      case v: BigDecimal => v.doubleValue()
      case v: Timestamp => v.getTime.toDouble
      case v: Date => v.getTime.toDouble
      case v: Instant => v.toEpochMilli.toDouble
    }
  }

  override def vectorTransform(length: Int): DenseVector[Double] = {
    val result = (vector - mn) * scale
    if (length < result.length) result.slice(0, length)
    else result
  }

  override def clear(): Unit = {
    vector = DenseVector.zeros[Double](columnSize)
  }

}

class VectorizedIdentityToZeroTransformation(
    override val transformation: Transformation,
    override val columnPosition: Int,
    override val columnSize: Int)
    extends VectorTransformation {

  private val vector: DenseVector[Double] = DenseVector.zeros[Double](columnSize)

  override def updateVector(i: Int, value: Any): Unit = {}

  override def vectorTransform(length: Int): DenseVector[Double] = {
    if (length < columnSize) vector.slice(0, length)
    else vector
  }

  override def clear(): Unit = {}
}
