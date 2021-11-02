/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.model.{Weight, NormalizedWeight}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
 * Aggregation object that estimates MaxWeight on a DataFrame
 */
object MaxWeightEstimation
    extends Aggregator[NormalizedWeight, NormalizedWeight, NormalizedWeight] {

  /**
   * Zero value for this aggregation
   * @return Normalized value of minimum Weight
   */
  override def zero: NormalizedWeight = Weight.MinValue.fraction

  /**
   * Combine two values to produce a new value.
   * For performance, the function may modify `buffer`
   * and return it instead of constructing a new object
   * @param buffer intermediate value for reduction
   * @param weight input from aggregation
   * @return merge of normalized weights buffer and weight
   */
  override def reduce(buffer: NormalizedWeight, weight: NormalizedWeight): Double = {
    NormalizedWeight.merge(buffer, weight)
  }

  /**
   * Merges two intermediate values
   * @param w1 intermediate result
   * @param w2 intermediate result
   * @return merge of intermediate result w1 and w2
   */
  override def merge(w1: NormalizedWeight, w2: NormalizedWeight): Double = {
    NormalizedWeight.merge(w1, w2)
  }

  /**
   * Transforms the output of the reduction
   * @param reduction final buffer
   * @return the final output result
   */
  override def finish(reduction: NormalizedWeight): NormalizedWeight = reduction

  /**
   * Specifies the Encoder for the intermediate value type
   */
  override def bufferEncoder: Encoder[NormalizedWeight] = Encoders.scalaDouble

  /**
   * Specifies the Encoder for the final output value type
   * @return the Encoder for the output
   */
  override def outputEncoder: Encoder[NormalizedWeight] = Encoders.scalaDouble
}
