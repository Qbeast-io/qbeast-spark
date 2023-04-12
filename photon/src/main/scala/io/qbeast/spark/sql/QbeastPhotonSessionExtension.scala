package io.qbeast.spark.sql

import io.qbeast.spark.sql.execution.rules.V2ScanToLogicalRelation
import org.apache.spark.sql.SparkSessionExtensions

class QbeastPhotonSessionExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy { session =>
      new V2ScanToLogicalRelation(session)
    }
  }

}
