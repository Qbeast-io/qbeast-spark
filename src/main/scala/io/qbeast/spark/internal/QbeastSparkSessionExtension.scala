/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import io.delta.sql.DeltaSparkSessionExtension
import io.qbeast.spark.internal.rules.{QbeastAnalysis, SampleRule, SaveAsTableRule}
import org.apache.spark.sql.SparkSessionExtensions

/**
 * Qbeast rules extension to spark query analyzer/optimizer/planner
 */
class QbeastSparkSessionExtension extends DeltaSparkSessionExtension {

  override def apply(extensions: SparkSessionExtensions): Unit = {

    super.apply(extensions)

    extensions.injectResolutionRule { session =>
      new QbeastAnalysis(session)
    }

    extensions.injectOptimizerRule { session =>
      SampleRule
    }

    extensions.injectOptimizerRule { session =>
      new SaveAsTableRule(session)
    }
  }

}
