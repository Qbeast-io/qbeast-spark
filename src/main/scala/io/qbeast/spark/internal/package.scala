/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark

import org.apache.spark.sql.catalyst.expressions.Expression

package object internal {
  type SparkPlan = Seq[Expression]
}
