/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
