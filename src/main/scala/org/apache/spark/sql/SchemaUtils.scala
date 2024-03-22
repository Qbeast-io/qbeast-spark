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
package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

object SchemaUtils {

  /**
   * Sets all the fields from the Schema to nullable
   * @param schema
   *   the schema to change
   * @return
   */
  def schemaAsNullable(schema: StructType): StructType = {
    schema.asNullable
  }

  /**
   * Whether the newSchema is read compatible with the currentSchema or not
   * @param newSchema
   *   the schema to test
   * @param currentSchema
   *   the current schema of the table
   * @return
   */
  def isReadCompatible(newSchema: StructType, currentSchema: StructType): Boolean = {
    org.apache.spark.sql.delta.schema.SchemaUtils.isReadCompatible(newSchema, currentSchema)
  }

}
