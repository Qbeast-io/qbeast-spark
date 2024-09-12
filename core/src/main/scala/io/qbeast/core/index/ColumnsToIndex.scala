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
package io.qbeast.core.index

/**
 * Utility object to work with the columns to index.
 */
object ColumnsToIndex {
  private val separator = ","

  /**
   * Decodes columns to index from a given string.
   *
   * @param string
   *   the string to decode
   * @return
   *   the decoded columns to index
   */
  def decode(string: String): Seq[String] = string.split(separator).toSeq

  /**
   * Encodes given columns to index to a single string.
   *
   * @param columnsToIndex
   *   the columns to index
   * @return
   *   the encoded columns to index
   */
  def encode(columnsToIndex: Seq[String]): String = columnsToIndex.mkString(separator)

}
