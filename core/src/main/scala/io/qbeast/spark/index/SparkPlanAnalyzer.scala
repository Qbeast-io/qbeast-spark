package io.qbeast.spark.index

import io.qbeast.core.model.Revision
import io.qbeast.spark.internal.rules.QbeastRelation
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LocalLimit
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Sample
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.DataFrame

import scala.collection.convert.ImplicitConversions.`collection asJava`

trait SparkPlanAnalyzer {

  /**
   * Builds a list of non-deterministic operations in the logical plan
   *
   * A list of Non-Deterministic operations:
   *   - LocalLimit without Sort Operation
   *   - Sample
   *   - Filter with non-deterministic condition
   *
   * @param plan
   *   the logical plan
   * @return
   */
  private def isLogicalPlanDeterministic(plan: LogicalPlan): Boolean = {
    // Recursively traverse the logical plan to find non-deterministic operations
    val isCurrentOperationDeterministic = plan match {
      case LocalLimit(_, _: Sort) => true // LocalLimit with Sort is deterministic
      case LocalLimit(_, _) => false
      case Sample(_, _, false, _, child) =>
        child match {
          case QbeastRelation(_, _) =>
            true // Sample over QbeastRelation is deterministic
          case _ => false
        }
      case Filter(condition, _) => condition.deterministic
      case _ => true
    }

    val areChildOperationsDeterministic = plan.children.forall(isLogicalPlanDeterministic)

    isCurrentOperationDeterministic && areChildOperationsDeterministic
  }

  /**
   * Extracts the select expressions from the logical plan for the provided column name
   *
   * @param logicalPlan
   *   the logical plan
   * @param columnName
   *   the column name
   * @return
   */
  private[index] def collectSelectExpressions(
      logicalPlan: LogicalPlan,
      columnName: String): Seq[Expression] = {
    logicalPlan match {
      case Project(projectList, _) =>
        projectList.collect {
          case Alias(child, name) if name == columnName => child
          case expr if expr.references.map(_.name).contains(columnName) => expr
        }
      case _ =>
        logicalPlan.children.flatMap(child => collectSelectExpressions(child, columnName))
    }
  }

  /**
   * Checks if a column is deterministic in the logical plan
   * @param logicalPlan
   *   the logical plan
   * @param columnName
   *   the column name
   * @return
   */

  private def isColumnDeterministic(logicalPlan: LogicalPlan, columnName: String): Boolean = {
    val expressionSet = collectSelectExpressions(logicalPlan, columnName)
    expressionSet.forall(_.deterministic)
  }

  /**
   * Analyzes the DataFrame to determine if it's execution is safely deterministic for indexing
   *
   *   - The logical plan of the DataFrame is checked for determinism
   *   - The columns to index are checked for determinism
   *
   * @param dataFrame
   *   the DataFrame to analyze
   * @param revision
   *   the Revision to analyze
   * @return
   */
  def analyzeDataFrameDeterminism(dataFrame: DataFrame, revision: Revision): Boolean = {
    // Access the logical plan of the DataFrame
    val logicalPlan: LogicalPlan = dataFrame.queryExecution.logical

    // Check if the logical plan's query is deterministic
    // Detect if the DataFrame's operations are deterministic
    val isQueryDeterministic: Boolean = isLogicalPlanDeterministic(logicalPlan)

    // Check if any of the columns to index in the DataFrame is deterministic
    val columnsToIndex = revision.columnTransformers.map(_.columnName)
    val areColumnsToIndexDeterministic: Boolean =
      columnsToIndex.forall(column => isColumnDeterministic(logicalPlan, column))

    // Check if the source is deterministic
    isQueryDeterministic && areColumnsToIndexDeterministic
  }

}
