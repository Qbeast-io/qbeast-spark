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
package io.qbeast.spark.hudi

import io.qbeast.core.model.mapper
import io.qbeast.spark.metadata.MetadataOperation
import io.qbeast.spark.utils.MetadataConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.StoragePath

/**
 * Qbeast metadata changes on a Delta Table.
 */
private[hudi] trait HudiMetadataOperation extends MetadataOperation {

  /**
   * Update Qbeast Metadata
   * @param isOverwriteMode
   *   whether the write mode is overwritten
   * @param configuration
   *   the changes in the table
   * @param hasRevisionUpdate
   *   the Qbeast options to update
   */
  def updateTableMetadata(
      metaClient: HoodieTableMetaClient,
      isOverwriteMode: Boolean,
      configuration: Configuration,
      hasRevisionUpdate: Boolean): Unit = {

    if (!hasRevisionUpdate)
      return

    val tableProperties = metaClient.getTableConfig

    val configJson = mapper.writeValueAsString(configuration)
    tableProperties.setValue(MetadataConfig.configuration, configJson)

    val metaPathDir =
      new StoragePath(metaClient.getBasePathV2, HoodieTableMetaClient.METAFOLDER_NAME)

    HoodieTableConfig.create(metaClient.getStorage, metaPathDir, tableProperties.getProps())
  }

}
