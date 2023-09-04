/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

object SchemaUtils {

  /**
   * Sets all the fields from the Schema to nullable
   * @param schema the schema to change
   * @return
   */
  def schemaAsNullable(schema: StructType): StructType = {
    schema.asNullable
  }

  /**
   * Whether the newSchema is read compatible with the currentSchema or not
   * @param newSchema the schema to test
   * @param currentSchema the current schema of the table
   * @return
   */
  def isReadCompatible(newSchema: StructType, currentSchema: StructType): Boolean = {
    org.apache.spark.sql.delta.schema.SchemaUtils.isReadCompatible(newSchema, currentSchema)
  }

}
