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

import io.qbeast.spark.internal.QbeastOptions
import org.apache.spark.sql.DataFrame

/**
 * Metadata Manager template
 */
trait StagingDataManager {

  /**
   * Resolve write policy according to the current staging size and its desired value.
   *
   * @param data
   *   DataFrame to write.
   * @return
   *   A StagingResolution instance containing the data to write, the staging RemoveFiles, and a
   *   boolean denoting whether the data to write is to be staged or indexed.
   */
  def updateWithStagedData(data: DataFrame): StagingResolution

  /**
   * Stage the data without indexing by writing it in the desired format.
   *
   * @param data
   *   The data to stage.
   * @param indexStatus
   *   The index status.
   * @param options
   *   The options for staging.
   * @param append
   *   Whether the operation appends data or overwrites.
   */
  def stageData(
      data: DataFrame,
      indexStatus: IndexStatus,
      options: QbeastOptions,
      append: Boolean): Unit

}

trait StagingDataManagerFactory {

  /**
   * Returns a IndexedTable for given SQLContext and path. It is not guaranteed that the returned
   * table physically exists, use IndexedTable#exists attribute to verify it.
   *
   * @param tableId
   *   the table path
   * @return
   *   the stagingmanager
   */
  def getManager(tableId: QTableID): StagingDataManager
}

case class StagingResolution(
    dataToWrite: DataFrame,
    removeFiles: Seq[DeleteFile],
    sendToStaging: Boolean)
