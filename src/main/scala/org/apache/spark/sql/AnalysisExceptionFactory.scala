/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql

import org.apache.spark.QueryContext

/**
 * Static factory for creating AnalysisException instances. This class allows to use the package
 * private constructor of AnalysisException directly.
 */
object AnalysisExceptionFactory {

  /**
   * Creates a new AnalysisException instance for given message, line, startPosition, plan and
   * cause.
   *
   * @param message
   *   the message
   * @param line
   *   the line
   * @param startPosition
   *   the start position
   * @param plan
   *   the plan
   * @param cause
   *   the cause
   * @return
   *   a new instance
   */
  def create(
      message: String,
      line: Option[Int] = None,
      startPosition: Option[Int] = None,
      cause: Option[Throwable] = None,
      errorClass: Option[String] = None,
      messageParameters: Map[String, String] = Map.empty,
      context: Array[QueryContext] = Array.empty): AnalysisException =
    new AnalysisException(
      message = message,
      line = line,
      startPosition = startPosition,
      context = context,
      cause = cause,
      errorClass = errorClass,
      messageParameters = messageParameters)

}
