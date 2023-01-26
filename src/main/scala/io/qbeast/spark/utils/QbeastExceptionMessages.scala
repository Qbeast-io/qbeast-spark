/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.utils

object QbeastExceptionMessages {

  /**
   * Conversion error for attempting to convert a partitioned table
   */
  val partitionedTableExceptionMsg: String =
    """Converting a partitioned table into qbeast is not supported.
      |Consider overwriting the entire data using qbeast.""".stripMargin.replaceAll("\n", " ")

  /**
   * Conversion error for unsupported file format
   * @return Exception message with the input file format
   */
  def unsupportedFormatExceptionMsg: String => String = (fileFormat: String) =>
    s"Unsupported file format: $fileFormat"

  /**
   * Conversion error for incorrect identifier format
   * @return
   */
  def incorrectIdentifierFormat: String => String = (identifier: String) =>
    "Required table identifier format: fileFormat.`<tablePath>` " +
      s"identifier received: $identifier"

}
