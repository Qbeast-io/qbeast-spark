package io.qbeast.spark.sql.execution

case class SampleOperator(
    lowerBound: Double,
    upperBound: Double,
    withReplacement: Boolean,
    seed: Long)
