package io.qbeast.spark.internal.rules

import io.qbeast.spark.internal.sources.v2.QbeastTableImpl
import org.apache.spark.sql.{AnalysisExceptionFactory, SchemaUtils, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AnsiCast,
  ArrayTransform,
  Attribute,
  Cast,
  CreateStruct,
  Expression,
  GetArrayItem,
  GetStructField,
  LambdaFunction,
  NamedExpression,
  NamedLambdaVariable,
  UpCast
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaErrors, DeltaLog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructField, StructType}

trait QbeastAnalysisUtils {

  private lazy val session = SparkSession.active

  /**
   * Checks if the schema of the Table corresponds to the schema of the Query
   * From Delta Lake OSS Project code in DeltaAnalysis
   *
   * @param tableName
   * @param query
   * @param schema
   * @return
   */
  protected def needSchemaAdjustment(
      tableName: String,
      query: LogicalPlan,
      schema: StructType): Boolean = {
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

  def insertIntoByNameMissingColumn(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      qbeastTable: QbeastTableImpl): Unit = {

    val deltaLog = DeltaLog.forTable(session, qbeastTable.v1Table.location.toString)
    val deltaLogProtocol = deltaLog.update().protocol
    if (query.output.length < targetAttrs.length) {
      // Some columns are not specified. We don't allow schema evolution in INSERT INTO BY NAME, so
      // we need to ensure the missing columns must be generated columns.
      val userSpecifiedNames = if (session.sessionState.conf.caseSensitiveAnalysis) {
        query.output.map(a => (a.name, a)).toMap
      } else {
        CaseInsensitiveMap(query.output.map(a => (a.name, a)).toMap)
      }
      val tableSchema = qbeastTable.schema()
      if (tableSchema.length != targetAttrs.length) {
        // The target attributes may contain the metadata columns by design. Throwing an exception
        // here in case target attributes may have the metadata columns for Delta in future.
        throw DeltaErrors.schemaNotConsistentWithTarget(s"$tableSchema", s"$targetAttrs")
      }
      qbeastTable.schema().foreach { col =>
        if (!userSpecifiedNames.contains(col.name) &&
          !ColumnWithDefaultExprUtils.columnHasDefaultExpr(deltaLogProtocol, col)) {
          throw DeltaErrors.missingColumnsInInsertInto(col.name)
        }
      }
    }
  }

  protected def resolveQueryColumnsByName(
      query: LogicalPlan,
      targetAttrs: Seq[Attribute],
      qbeastTable: QbeastTableImpl): LogicalPlan = {
    insertIntoByNameMissingColumn(query, targetAttrs, qbeastTable)
    // Spark will resolve columns to make sure specified columns are in the table schema and don't
    // have duplicates. This is just a sanity check.
    assert(
      query.output.length <= targetAttrs.length,
      s"Too many specified columns ${query.output.map(_.name).mkString(", ")}. " +
        s"Table columns: ${targetAttrs.map(_.name).mkString(", ")}")

    val project = query.output.map { attr =>
      val targetAttr = targetAttrs
        .find(t => session.sessionState.conf.resolver(t.name, attr.name))
        .getOrElse {
          // This is a sanity check. Spark should have done the check.
          throw AnalysisExceptionFactory.create("Missing column")
        }
      addCastToColumn(attr, targetAttr, qbeastTable.name())
    }
    Project(project, query)
  }

  protected def resolveQueryColumnsByOrdinal(
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

  protected def getCastFunction: CastFunction = {
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

  def addCastsToStructs(
      tableName: String,
      parent: NamedExpression,
      source: StructType,
      target: StructType): NamedExpression = {
    if (source.length < target.length) {
      throw DeltaErrors.notEnoughColumnsInInsert(
        tableName,
        source.length,
        target.length,
        Some(parent.qualifiedName))
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
            throw DeltaErrors.cannotInsertIntoColumn(tableName, field, targetName, o.simpleString)
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
      val indexVar = NamedLambdaVariable("indexVar", IntegerType, false)
      LambdaFunction(structConverter(elementVar, indexVar), Seq(elementVar, indexVar))
    }
    ArrayTransform(parent, transformLambdaFunc)
  }

  protected def addCastToColumn(
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
        getCastFunction(attr, targetAttr.dataType)
    }
    Alias(expr, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
  }

}
