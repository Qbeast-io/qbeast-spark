package io.qbeast.spark.utils

import io.qbeast.spark.internal.rules.QbeastRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.DataFrame

trait SparkPlanAnalyzer {

  type PlanOrExpression = Either[LogicalPlan, Expression]

  protected def collectFirst[In, Out](
      input: Iterable[In],
      recurse: In => Option[Out]): Option[Out] = {
    input.foldLeft(Option.empty[Out]) { case (acc, value) =>
      acc.orElse(recurse(value))
    }
  }

  /**
   * From Delta's DeltaSparkPlanUtils.scala
   *
   * Given a list of expressions, this method finds the first non-deterministic node in the
   * @param children
   * @return
   */
  protected def findFirstNonDeterministicChildNode(
      children: Seq[Expression]): Option[PlanOrExpression] =
    collectFirst[Expression, PlanOrExpression](children, findFirstNonDeterministicNode)

  /**
   * From Delta's DeltaSparkPlanUtils.scala
   *
   * Given an expression, this method finds the first non-deterministic node in the expression
   * tree
   * @param child
   * @return
   */

  protected def findFirstNonDeterministicNode(child: Expression): Option[PlanOrExpression] = {
    child match {
      case SubqueryExpression(plan) =>
        findFirstNonDeterministicNode(plan)
      case otherPlan =>
        collectFirst[Expression, PlanOrExpression](
          otherPlan.children,
          findFirstNonDeterministicNode) orElse {
          if (otherPlan.deterministic) None
          else Some(Right(otherPlan)) // Is this a deterministic expression?
        }
    }
  }

  protected def checkNonDeterminismForSample(sample: Sample): Option[PlanOrExpression] = {
    sample.child match {
      case QbeastRelation(_, _) => None // Deterministic Sample with Qbeast
      case _: OneRowRelation => None
      case node => Some(Left(node))
    }

  }

  /**
   * From Delta's DeltaSparkPlanUtils.scala
   * @param plan
   * @return
   */
  protected def findFirstNonDeterministicNode(plan: LogicalPlan): Option[PlanOrExpression] = {
    plan match {
      case Distinct(child) => findFirstNonDeterministicNode(child)
      case Project(projectList, child) =>
        findFirstNonDeterministicChildNode(projectList) orElse {
          findFirstNonDeterministicNode(child)
        }
      case Filter(cond, child) =>
        findFirstNonDeterministicNode(cond) orElse {
          findFirstNonDeterministicNode(child)
        }
      case Union(children, _, _) =>
        collectFirst[LogicalPlan, PlanOrExpression](children, findFirstNonDeterministicNode)
      case SubqueryAlias(_, child) => findFirstNonDeterministicNode(child)
      case s: Sample => checkNonDeterminismForSample(s)
      case GlobalLimit(_, child) => findFirstNonDeterministicNode(child)
      case LocalLimit(_, _: Sort) => None
      case LocalLimit(_, _) => Some(Left(plan)) // LocalLimit is non-deterministic
      case OneRowRelation() => None
      case _: Range => None
      case leaf: LeafNode => if (leaf.deterministic) None else Some(Left(leaf))
      case other => Some(Left(other))
    }
  }

  /**
   * Verifies if the logical plan is deterministic
   *
   * A plan is Undeterministic if any of the nodes is not deterministic. This method finds the
   * first non-deterministic node in the logical plan, and would return false if it exists
   *
   * @param plan
   *   the logical plan
   * @return
   */
  protected def isLogicalPlanDeterministic(plan: LogicalPlan): Boolean = {
    findFirstNonDeterministicNode(plan).isEmpty
  }

  /**
   * Checks if the logical plan of the DataFrame is deterministic
   * @param dataFrame
   * @return
   */
  protected def isDataFramePlanDeterministic(dataFrame: DataFrame): Boolean = {
    isLogicalPlanDeterministic(dataFrame.queryExecution.logical)
  }

}
