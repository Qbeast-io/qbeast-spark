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
package io.qbeast.spark.rules

import org.apache.spark.sql.catalyst.analysis.TableOutputResolver
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.ArrayTransform
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.CreateStruct
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GetArrayItem
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.catalyst.expressions.LambdaFunction
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.NamedLambdaVariable
import org.apache.spark.sql.catalyst.expressions.UpCast
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.SchemaUtils

private[rules] object QbeastAnalysisUtils {

  private lazy val conf = SQLConf.get

  /**
   * Checks if the schema of the Table corresponds to the schema of the Query From Delta Lake OSS
   * Project code in DeltaAnalysis
   *
   * @param tableName
   * @param query
   * @param schema
   * @return
   */
  def needSchemaAdjustment(tableName: String, query: LogicalPlan, schema: StructType): Boolean = {
    val output = query.output
    if (output.length < schema.length) {
      throw AnalysisExceptionFactory.create(
        "The number of colums to write does not correspond " +
          s"to the number of columns of the Table $tableName")
    }
    // Now we should try our best to match everything that already exists, and leave the rest
    // for schema evolution
    val existingSchemaOutput = output.take(schema.length)

    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
    !SchemaUtils.isReadCompatible(
      SchemaUtils.schemaAsNullable(schema),
      existingSchemaOutput.toStructType)
  }

  /**
   * From DeltaAnalysis code in
   * spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala Performs the schema
   * adjustment by adding UpCasts (which are safe) and Aliases so that we can check if the schema
   * of the insert query matches our table
   * @param query
   *   the input query for the insert
   * @param targetAttrs
   *   the target attributes
   * @param tblName
   *   the name of the table
   * @return
   */
  def resolveQueryColumnsByOrdinal(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      tblName: String): LogicalPlan = {
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        addCastToColumn(attr, targetAttr, tblName)
      } else {
        attr
      }
    }
    Project(project, query)
  }

  type CastFunction = (Expression, DataType, String) => Expression

  /**
   * From DeltaAnalysis code in
   * spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala Get cast operation for
   * the level of strictness in the schema a user asked for
   * @return
   */
  def getCastFunction: CastFunction = {
    val timeZone = conf.sessionLocalTimeZone
    conf.storeAssignmentPolicy match {
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        (input: Expression, dt: DataType, _) =>
          Cast(input, dt, Option(timeZone), ansiEnabled = false)
      case SQLConf.StoreAssignmentPolicy.ANSI =>
        (input: Expression, dt: DataType, name: String) => {
          val cast = Cast(input, dt, Option(timeZone), ansiEnabled = true)
          cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
          TableOutputResolver.checkCastOverflowInTableInsert(cast, name)
        }
      case SQLConf.StoreAssignmentPolicy.STRICT =>
        (input: Expression, dt: DataType, _) => UpCast(input, dt)
    }
  }

  /**
   * From DeltaAnalysis code in
   * spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala Recursively casts structs
   * in case it contains null types. TODO: Support other complex types like MapType and ArrayType
   * @param tableName
   *   the name of the table
   * @param parent
   *   the parent expression to cast
   * @param source
   *   the source schema
   * @param target
   *   the target schema
   * @return
   *   The casted expression
   */
  def addCastsToStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType): NamedExpression = {
    if (source.length < target.length) {

      throw AnalysisExceptionFactory.create(
        s"Not enough columns in INSERT INTO $tableName. " +
          s"Found ${source.length} columns and need ${target.length}")
    }
    val fields = source.zipWithIndex.map {
      case (StructField(name, nested: StructType, _, metadata), i) if i < target.length =>
        target(i).dataType match {
          case t: StructType =>
            val subField =
              Alias(GetStructField(parent, i, Option(name)), target(i).name)(explicitMetadata =
                Option(metadata))
            addCastsToStructs(tableName, subField, nested, t)
          case o =>
            val field = parent.qualifiedName + "." + name
            val targetName = parent.qualifiedName + "." + target(i).name
            throw AnalysisExceptionFactory.create(
              s"Cannot insert into column $field in table $tableName. Use $targetName instead")
        }
      case (other, i) if i < target.length =>
        val targetAttr = target(i)
        Alias(
          getCastFunction(
            GetStructField(parent, i, Option(other.name)),
            targetAttr.dataType,
            targetAttr.name),
          targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))

      case (other, i) =>
        // This is a new column, so leave to schema evolution as is. Do not lose it's name so
        // wrap with an alias
        Alias(GetStructField(parent, i, Option(other.name)), other.name)(explicitMetadata =
          Option(other.metadata))
    }
    Alias(CreateStruct(fields), parent.name)(
      parent.exprId,
      parent.qualifier,
      Option(parent.metadata))
  }

  /**
   * From DeltaAnalysis code in
   * spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala
   *
   * Recursively add casts to Array[Struct]
   * @param tableName
   *   the name of the table
   * @param parent
   *   the parent expression
   * @param source
   *   the source Struct
   * @param target
   *   the final target Struct
   * @param sourceNullable
   *   if source is nullable
   * @return
   */

  private def addCastsToArrayStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType,
      sourceNullable: Boolean): Expression = {
    val structConverter: (Expression, Expression) => Expression = (_, i) =>
      addCastsToStructs(tableName, Alias(GetArrayItem(parent, i), i.toString)(), source, target)
    val transformLambdaFunc = {
      val elementVar = NamedLambdaVariable("elementVar", source, sourceNullable)
      val indexVar = NamedLambdaVariable("indexVar", IntegerType, nullable = false)
      LambdaFunction(structConverter(elementVar, indexVar), Seq(elementVar, indexVar))
    }
    ArrayTransform(parent, transformLambdaFunc)
  }

  /**
   * From DeltaAnalysis code in
   * spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala Adds cast to input/query
   * column from the target table
   * @param attr
   *   the column to cast in Attribute form
   * @param targetAttr
   *   the target column of the table
   * @param tblName
   *   the name of the table
   * @return
   */
  def addCastToColumn(
      attr: Attribute,
      targetAttr: Attribute,
      tblName: String): NamedExpression = {
    val expr = (attr.dataType, targetAttr.dataType) match {
      case (s, t) if s == t =>
        attr
      case (s: StructType, t: StructType) if s != t =>
        addCastsToStructs(tblName, attr, s, t)
      case (ArrayType(s: StructType, sNull: Boolean), ArrayType(t: StructType, tNull: Boolean))
          if s != t && sNull == tNull =>
        addCastsToArrayStructs(tblName, attr, s, t, sNull)
      case _ =>
        getCastFunction(attr, targetAttr.dataType, targetAttr.name)
    }
    Alias(expr, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
  }

}
