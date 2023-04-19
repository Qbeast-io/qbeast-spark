package io.qbeast.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Container class for the pushdown query operators
 * @param aggregation pushed aggregation, if any
 * @param sample pushed sample, if any
 * @param filters sequence of pushed filters
 */
case class QueryOperators(
    sample: Option[SampleOperator],
    filters: Seq[Expression])
