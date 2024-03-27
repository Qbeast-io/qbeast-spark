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
package io.qbeast.core.model

/**
 * ColumnsToIndexSelector interface to automatically select which columns to index.
 * @tparam DATA
 *   the data to index
 */
trait ColumnsToIndexSelector[DATA] {

  /**
   * The maximum number of columns to index.
   * @return
   */
  def MAX_COLUMNS_TO_INDEX: Int

  /**
   * Selects the columns to index given a DataFrame
   * @param data
   *   the data to index
   * @return
   */
  def selectColumnsToIndex(data: DATA): Seq[String] =
    selectColumnsToIndex(data, MAX_COLUMNS_TO_INDEX)

  /**
   * Selects the columns to index with a given number of columns to index
   * @param data
   *   the data to index
   * @param numColumnsToIndex
   *   the number of columns to index
   * @return
   *   A sequence with the names of the columns to index
   */
  def selectColumnsToIndex(data: DATA, numColumnsToIndex: Int): Seq[String]

}
