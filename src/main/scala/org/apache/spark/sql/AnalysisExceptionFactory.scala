/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Static factory for creating AnalysisException instances. This class allows to use
 * the package private constructor of AnalysisException directly.
 */
object AnalysisExceptionFactory {

  /**
   * Creates a new AnalysisException instance for
   * given message, line, startPosition, plan and cause.
   *
   * @param message the message
   * @param line the line
   * @param startPosition the start position
   * @param plan the plan
   * @param cause the cause
   * @return a new instance
   */
  def create(
      message: String,
      line: Option[Int] = None,
      startPosition: Option[Int] = None,
      plan: Option[LogicalPlan] = None,
      cause: Option[Throwable] = None): AnalysisException =
    new AnalysisException(
      message = message,
      line = line,
      startPosition = startPosition,
      plan = plan,
      cause = cause)

}
