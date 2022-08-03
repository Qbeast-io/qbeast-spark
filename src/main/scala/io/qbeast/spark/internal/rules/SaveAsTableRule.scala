package io.qbeast.spark.internal.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

class SaveAsTableRule(spark: SparkSession) extends Rule[LogicalPlan] with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // When CreateTableAsSelect statement is in place for qbeast
    // We need to pass the writeOptions as properties to the creation of the table
    plan transformDown {
      case saveAsSelect: CreateTableAsSelect
          if saveAsSelect.properties.get("provider").contains("qbeast") =>
        val options = saveAsSelect.writeOptions
        val finalProperties = saveAsSelect.properties ++ options
        saveAsSelect.copy(properties = finalProperties)
    }
  }

}
