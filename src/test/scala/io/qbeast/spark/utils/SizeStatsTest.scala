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
package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.functions.col

class SizeStatsTest extends QbeastIntegrationTestSpec {

  "SizeStatsTest" should "work with longs" in withSpark { spark =>
    import spark.implicits._

    val sizeStats = spark
      .range(1001)
      .select(SizeStats.forColumn(col("id")).as("stats"))
      .select("stats.*")
      .as[SizeStats]
      .first()

    sizeStats shouldBe SizeStats(1001, 500, 289, Seq(0, 250, 500, 750, 1000))
  }

}
