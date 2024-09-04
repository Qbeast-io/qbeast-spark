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

import io.qbeast.core.utils.StagingUtils
import io.qbeast.spark.internal.QbeastOptions
import org.apache.spark.sql.DataFrame

import java.util.ServiceLoader

/**
 * Metadata Manager template
 * @tparam DataSchema
 *   type of data schema
 * @tparam FileDescriptor
 *   type of file descriptor
 * @tparam QbeastOptions
 *   type of the Qbeast options
 */
trait StagingDataManager[T] extends StagingUtils {

  /**
   * Stack a given DataFrame with all staged data.
   *
   * @param data
   *   The DataFrame to merge with staged data.
   * @param stagedFiles
   *   The sequence of staged files to merge.
   * @return
   *   The merged DataFrame.
   */
  def mergeWithStagingData(data: DataFrame, stagedFiles: Seq[T]): DataFrame

  /**
   * Resolve write policy according to the current staging size and its desired value.
   *
   * @param data
   *   DataFrame to write.
   * @return
   *   A StagingResolution instance containing the data to write, the staging RemoveFiles, and a
   *   boolean denoting whether the data to write is to be staged or indexed.
   */
  def updateWithStagedData(data: DataFrame): StagingResolution[T]

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

case class StagingResolution[T](
    dataToWrite: DataFrame,
    removeFiles: Seq[T],
    sendToStaging: Boolean)

object StagingDataManager {

  /**
   * Creates a StagingDataManager instance for a given configuration.
   *
   * @param config
   *   the configuration
   * @return
   *   a StagingDataManager instance
   */
  def apply[FileDescriptor](tableID: QTableID): StagingDataManager[FileDescriptor] = {
    val loader = ServiceLoader.load(classOf[StagingDataManagerFactory[FileDescriptor]])
    val iterator = loader.iterator()
    if (iterator.hasNext) {
      iterator.next().createStagingDataManager(tableID)
    } else {
      throw new IllegalStateException(
        "No StagingDataManagerFactory found for the given configuration")
    }
  }

}

/**
 * Factory for creating StagingDataManager instances. This interface should be implemented and
 * deployed by external libraries as follows: <ul> <li>Implement this interface in a class which
 * has a public no-argument constructor</li> <li>Register the implementation according to the
 * ServiceLoader specification</li> <li>Add the jar with the implementation to the application
 * classpath</li> </ul>
 */
trait StagingDataManagerFactory[FileDescriptor] {

  /**
   * Creates a new StagingDataManager for a given configuration.
   *
   * @param tableID
   *   the table identifier
   * @param config
   *   the configuration
   * @return
   *   a new StagingDataManager
   */
  def createStagingDataManager(tableID: QTableID): StagingDataManager[FileDescriptor]

  val format: String = ???
}
