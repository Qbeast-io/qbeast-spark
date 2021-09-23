/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Static factory for creating Dataset instances. This class allows
 * to use a the package private Dataset#apply method directly.
 */
object DatasetFactory {

  /**
   * Creates a new Dataset instance for given session and plan.
   *
   * @param sparkSession the session
   * @param logicalPlan the plan
   * @tparam T the dataset type
   * @return a new instance
   */
  def create[T: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[T] =
    Dataset[T](sparkSession, logicalPlan)

}
