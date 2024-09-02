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

import io.qbeast.core.keeper.Keeper

/**
 * Qbeast Core main components
 * @tparam DATA
 *   type of the data
 * @tparam DataSchema
 *   type of the data schema
 * @tparam QbeastOptions
 *   type of the Qbeast options
 * @tparam FileDescriptor
 *   type of the file descriptor
 */
trait QbeastCoreContext[DATA, DataSchema, QbeastOptions, FileDescriptor] {
  def metadataManager: MetadataManager[DataSchema, FileDescriptor, QbeastOptions]
  def dataWriter: DataWriter[DATA, DataSchema, FileDescriptor]
  def indexManager: IndexManager[DATA]
  def revisionBuilder: RevisionFactory[DataSchema, QbeastOptions]
  def columnSelector: ColumnsToIndexSelector[DATA]
  def keeper: Keeper

}

/**
 * RevisionFactory
 *
 * @tparam DataSchema
 *   type of the data schema
 * @tparam QbeastOptions
 *   type of the Qbeast options
 */
trait RevisionFactory[DataSchema, QbeastOptions] {

  /**
   * Create a new revision for a table with given parameters
   *
   * @param qtableID
   *   the table identifier
   * @param schema
   *   the schema
   * @param options
   *   the options
   * @return
   */
  def createNewRevision(qtableID: QTableID, schema: DataSchema, options: QbeastOptions): Revision

  /**
   * Create a new revision with given parameters from an old revision
   * @param qtableID
   *   the table identifier
   * @param schema
   *   the schema
   * @param options
   *   the options
   * @param oldRevision
   *   the old revision
   * @return
   */
  def createNextRevision(
      qtableID: QTableID,
      schema: DataSchema,
      options: QbeastOptions,
      oldRevision: RevisionID): Revision

}
