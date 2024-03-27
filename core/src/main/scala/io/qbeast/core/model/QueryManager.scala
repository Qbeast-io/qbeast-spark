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
 * Query Manager template
 * @tparam QUERY
 *   type of the query
 * @tparam DATA
 *   type of the data
 */
trait QueryManager[QUERY, DATA] {

  /**
   * Executes a query against a index
   * @param query
   *   the query
   * @param indexStatus
   *   the current index status
   * @return
   *   the result of the query
   */
  def query(query: QUERY, indexStatus: IndexStatus): DATA

}
