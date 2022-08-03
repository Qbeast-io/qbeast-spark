/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import io.delta.sql.DeltaSparkSessionExtension
import io.qbeast.spark.internal.rules.{SampleRule, SaveAsTableRule}
import org.apache.spark.sql.SparkSessionExtensions

/**
 * Qbeast rules extension to spark query analyzer/optimizer/planner
 */
class QbeastSparkSessionExtension extends DeltaSparkSessionExtension {

  override def apply(extensions: SparkSessionExtensions): Unit = {

    super.apply(extensions)

    extensions.injectOptimizerRule { session =>
      new SampleRule(session)
    }

    extensions.injectOptimizerRule { session =>
      new SaveAsTableRule(session)
    }
  }

}
