package io.qbeast.spark.sql.execution

/**
 * Container class for Sample Operator
 * @param lowerBound the sample lower bound
 * @param upperBound the sample upper bound
 * @param withReplacement either the sample is with or without replacement
 * @param seed the seed
 */
case class SampleOperator(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long)
