package io.qbeast.spark.internal.rules

import org.apache.spark.sql.{AnalysisExceptionFactory, SchemaUtils}
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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StructField, StructType}

trait QbeastAnalysisUtils {

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
