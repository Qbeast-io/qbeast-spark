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

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.types.StructField
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
    isReadCompatibleInternal(newSchema, currentSchema)
  }

  private def isReadCompatibleInternal(
      existingSchema: StructType,
      readSchema: StructType,
      forbidTightenNullability: Boolean = false,
      allowMissingColumns: Boolean = false,
      newPartitionColumns: Seq[String] = Seq.empty,
      oldPartitionColumns: Seq[String] = Seq.empty): Boolean = {

    def isNullabilityCompatible(existingNullable: Boolean, readNullable: Boolean): Boolean = {
      if (forbidTightenNullability) {
        readNullable || !existingNullable
      } else {
        existingNullable || !readNullable
      }
    }

    def isDatatypeReadCompatible(existing: DataType, newtype: DataType): Boolean = {
      (existing, newtype) match {
        case (e: StructType, n: StructType) =>
          isReadCompatibleInternal(e, n, forbidTightenNullability)
        case (e: ArrayType, n: ArrayType) =>
          // if existing elements are non-nullable, so should be the new element
          isNullabilityCompatible(e.containsNull, n.containsNull) &&
          isDatatypeReadCompatible(e.elementType, n.elementType)
        case (e: MapType, n: MapType) =>
          // if existing value is non-nullable, so should be the new value
          isNullabilityCompatible(e.valueContainsNull, n.valueContainsNull) &&
          isDatatypeReadCompatible(e.keyType, n.keyType) &&
          isDatatypeReadCompatible(e.valueType, n.valueType)
        case (a, b) => a == b
      }
    }

    def isStructReadCompatible(existing: StructType, newtype: StructType): Boolean = {
      val existingFields = toFieldMap(existing)
      // scalastyle:off caselocale
      val existingFieldNames = existing.fieldNames.map(_.toLowerCase).toSet
      assert(
        existingFieldNames.size == existing.length,
        "Delta table don't allow field names that only differ by case")
      val newFields = newtype.fieldNames.map(_.toLowerCase).toSet
      assert(
        newFields.size == newtype.length,
        "Delta table don't allow field names that only differ by case")
      // scalastyle:on caselocale

      if (!allowMissingColumns &&
        !(existingFieldNames.subsetOf(newFields) &&
          isPartitionCompatible(newPartitionColumns, oldPartitionColumns))) {
        // Dropped a column that was present in the DataFrame schema
        return false
      }
      newtype.forall { newField =>
        // new fields are fine, they just won't be returned
        existingFields.get(newField.name).forall { existingField =>
          // we know the name matches modulo case - now verify exact match
          (existingField.name == newField.name
          // if existing value is non-nullable, so should be the new value
          && isNullabilityCompatible(existingField.nullable, newField.nullable)
          // and the type of the field must be compatible, too
          && isDatatypeReadCompatible(existingField.dataType, newField.dataType))
        }
      }
    }

    isStructReadCompatible(existingSchema, readSchema)
  }

  private def isPartitionCompatible(
      newPartitionColumns: Seq[String],
      oldPartitionColumns: Seq[String]): Boolean = {
    (newPartitionColumns.isEmpty && oldPartitionColumns.isEmpty) ||
    (newPartitionColumns == oldPartitionColumns)
  }

  private def toFieldMap(
      fields: Seq[StructField],
      caseSensitive: Boolean = false): Map[String, StructField] = {
    val fieldMap = fields.map(field => field.name -> field).toMap
    if (caseSensitive) {
      fieldMap
    } else {
      CaseInsensitiveMap(fieldMap)
    }
  }

}
