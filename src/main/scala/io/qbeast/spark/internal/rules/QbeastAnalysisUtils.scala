/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.rules

import org.apache.spark.sql.{AnalysisExceptionFactory, SchemaUtils}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AnsiCast,
  Attribute,
  Cast,
  CreateStruct,
  Expression,
  GetStructField,
  NamedExpression,
  UpCast
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}

private[rules] object QbeastAnalysisUtils {

  /**
   * Checks if the schema of the Table corresponds to the schema of the Query
   * From Delta Lake OSS Project code in DeltaAnalysis
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
        s"The number of colums to write does not correspond " +
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
   * From DeltaAnalysis code in spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala
   * Performs the schema adjustment by adding UpCasts (which are safe)
   * and Aliases so that we can check if the schema of the insert query matches our table
   * @param query the input query for the insert
   * @param targetAttrs the target attributes
   * @param tblName the name of the table
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

  type CastFunction = (Expression, DataType) => Expression

  /**
   * From DeltaAnalysis code in spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala
   * Get cast operation for the level of strictness in the schema a user asked for
   * @return
   */
  def getCastFunction: CastFunction = {
    val conf = SQLConf.get
    val timeZone = conf.sessionLocalTimeZone
    conf.storeAssignmentPolicy match {
      case SQLConf.StoreAssignmentPolicy.LEGACY =>
        Cast(_, _, Option(timeZone), ansiEnabled = false)
      case SQLConf.StoreAssignmentPolicy.ANSI =>
        (input: Expression, dt: DataType) => {
          AnsiCast(input, dt, Option(timeZone))
        }
      case SQLConf.StoreAssignmentPolicy.STRICT => UpCast(_, _)
    }
  }

  /**
   * From DeltaAnalysis code in spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala
   * Recursively casts structs in case it contains null types.
   * TODO: Support other complex types like MapType and ArrayType
   * @param tableName the name of the table
   * @param parent the parent expression to cast
   * @param source the source schema
   * @param target the target schema
   * @return The casted expression
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
          getCastFunction(GetStructField(parent, i, Option(other.name)), targetAttr.dataType),
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
   * From DeltaAnalysis code in spark/src/main/scala/org/apache/spark/sql/delta/DeltaAnalysis.scala
   * Adds cast to input/query column from the target table
   * @param attr the column to cast in Attribute form
   * @param targetAttr the target column of the table
   * @param tblName the name of the table
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
      case _ =>
        getCastFunction(attr, targetAttr.dataType)
    }
    Alias(expr, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
  }

}
