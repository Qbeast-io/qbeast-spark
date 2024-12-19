package io.qbeast.spark.internal.rules

import io.qbeast.core.model.QbeastOptions
import io.qbeast.spark.index.DefaultFileIndex
import io.qbeast.IndexedColumns
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * QbeastRelation matching pattern
 */
object QbeastRelation {

  def unapply(plan: LogicalPlan): Option[(LogicalRelation, IndexedColumns)] = plan match {

    case l @ LogicalRelation(
          q @ HadoopFsRelation(o: DefaultFileIndex, _, _, _, _, parameters),
          _,
          _,
          _) =>
      val qbeastOptions = QbeastOptions(parameters)
      val columnsToIndex = qbeastOptions.columnsToIndexParsed.map(_.columnName)
      Some((l, columnsToIndex))
    case _ => None
  }

}
