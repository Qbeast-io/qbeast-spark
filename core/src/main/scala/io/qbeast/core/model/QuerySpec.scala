/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Query specification
 *
 * @param weightRange the weight range
 * @param querySpace the query space
 */
case class QuerySpec(weightRange: WeightRange, querySpace: QuerySpace) {

  def isSampling: Boolean =
    weightRange.from > Weight.MinValue || weightRange.to < Weight.MaxValue

}
