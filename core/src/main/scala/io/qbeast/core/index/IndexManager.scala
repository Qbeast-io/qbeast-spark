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

import io.qbeast.core.model._
import io.qbeast.IISeq

/**
 * Index Manager template
 * @tparam DATA
 *   type of data to index
 */
trait IndexManager[DATA] {

  /**
   * Indexes the data
   * @param data
   *   the data to index
   * @param indexStatus
   *   the current index status
   * @return
   *   the changes of the index and reorganization of data
   */
  def index(data: DATA, indexStatus: IndexStatus): (DATA, TableChanges)

  /**
   * Optimizes the index
   * @param data
   *   the data of the index
   * @param indexStatus
   *   the current index status
   * @return
   *   the changes on the index and reorganization of data
   */
  def optimize(data: DATA, indexStatus: IndexStatus): (DATA, TableChanges)

  /**
   * Analyzes the current index status
   * @param indexStatus
   *   the current index status
   * @return
   *   the sequence of cubes that should be optimized for improving performance
   */
  def analyze(indexStatus: IndexStatus): IISeq[CubeId]

}
