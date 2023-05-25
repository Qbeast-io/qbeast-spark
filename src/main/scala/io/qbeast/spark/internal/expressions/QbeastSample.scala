/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.expressions

import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, BooleanType}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import io.qbeast.core.model.WeightRange
import io.qbeast.core.model.Weight

/**
 * Qbeast sample expression is used to emulate sample push down for queries
 * which specify sampling clause. This expression has boolean type and is always
 * evaluated to true. Being used as part of filter this expression provides
 * the lower and higher sampling bounds to a FileIndex implementation.
 *
 * @param lowerBound the lower sampling bound, must be in [0,1]
 * @param upperBound the upper sampling bound, must be in [0,1]
 */
case class QbeastSample(lowerBound: Double, upperBound: Double)
    extends LeafExpression
    with CodegenFallback
    with Serializable {

  // foldable must be false, otherwise the filter will be removed from the plan
  override def foldable: Boolean = false

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = true

  override def dataType: DataType = BooleanType

  override def prettyName: String = "qbeast_sample"

  override def toString(): String = s"$prettyName($lowerBound, $upperBound)"

  def toWeightRange(): WeightRange = WeightRange(Weight(lowerBound), Weight(upperBound))

}
