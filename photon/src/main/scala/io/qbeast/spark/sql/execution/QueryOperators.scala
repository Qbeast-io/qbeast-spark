package io.qbeast.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation

case class QueryOperators(
    aggregation: Option[Aggregation],
    sample: Option[SampleOperator],
    filters: Seq[Expression])
