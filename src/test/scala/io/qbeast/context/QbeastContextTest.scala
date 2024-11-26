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
package io.qbeast.context

import io.qbeast.spark.delta.DeltaMetadataManager
import io.qbeast.spark.delta.DeltaRollupDataWriter
import io.qbeast.spark.delta.DeltaStagingDataManagerFactory
import io.qbeast.spark.index.SparkColumnsToIndexSelector
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.table.IndexedTableFactoryImpl
import io.qbeast.QbeastIntegrationTestSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastContextTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  it should "use the unmanaged context if provided" in withSpark { spark =>
    val indexedTableFactory = new IndexedTableFactoryImpl(
      SparkOTreeManager,
      DeltaMetadataManager,
      DeltaRollupDataWriter,
      DeltaStagingDataManagerFactory,
      SparkRevisionFactory,
      SparkColumnsToIndexSelector)
    val unmanaged = new QbeastContextImpl(
      config = spark.sparkContext.getConf,
      indexedTableFactory = indexedTableFactory)
    QbeastContext.setUnmanaged(unmanaged)
    QbeastContext.unsetUnmanaged() shouldBe Some(unmanaged)
  }

  it should "use the managed context after the unmanaged is unset" in withSpark { spark =>
    val indexedTableFactory = new IndexedTableFactoryImpl(
      SparkOTreeManager,
      DeltaMetadataManager,
      DeltaRollupDataWriter,
      DeltaStagingDataManagerFactory,
      SparkRevisionFactory,
      SparkColumnsToIndexSelector)
    val unmanaged = new QbeastContextImpl(
      config = spark.sparkContext.getConf,
      indexedTableFactory = indexedTableFactory)
    QbeastContext.setUnmanaged(unmanaged)
    QbeastContext.unsetUnmanaged() shouldBe Some(unmanaged)
  }

}
