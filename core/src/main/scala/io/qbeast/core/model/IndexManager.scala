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

import org.apache.spark.sql.DataFrame

/**
 * Index Manager template
 */
trait IndexManager {

  /**
   * Indexes the data
   * @param data
   *   the data to index
   * @param indexStatus
   *   the current index status
   * @return
   *   the changes of the index and reorganization of data
   */
  def index(
      data: DataFrame,
      indexStatus: IndexStatus,
      options: QbeastOptions): (DataFrame, TableChanges)

  /**
   * Optimizes the input data by reassigning cubes according to the current index status
   * @param data
   *   the data to optimize
   * @param indexStatus
   *   the current index status
   * @return
   *   the optimized data and the changes of the index
   */
  def optimize(data: DataFrame, indexStatus: IndexStatus): (DataFrame, TableChanges)
}
