/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Static factory for creating Dataset instances. This class allows to use a the package private
 * Dataset#apply method directly.
 */
object DatasetFactory {

  /**
   * Creates a new Dataset instance for given session and plan.
   *
   * @param sparkSession
   *   the session
   * @param logicalPlan
   *   the plan
   * @tparam T
   *   the dataset type
   * @return
   *   a new instance
   */
  def create[T: Encoder](sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[T] =
    Dataset[T](sparkSession, logicalPlan)

}
