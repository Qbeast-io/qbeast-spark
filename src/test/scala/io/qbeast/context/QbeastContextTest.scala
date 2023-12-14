/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.context

import io.qbeast.core.keeper.Keeper
import io.qbeast.core.keeper.LocalKeeper
import io.qbeast.spark.delta.writer.RollupDataWriter
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.index.SparkColumnsToIndexSelector
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.table.IndexedTableFactoryImpl
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastContextTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  it should "use the unmanaged context if provided" in withSpark { spark =>
    val keeper = LocalKeeper
    val indexedTableFactory = new IndexedTableFactoryImpl(
      keeper,
      SparkOTreeManager,
      SparkDeltaMetadataManager,
      RollupDataWriter,
      SparkRevisionFactory,
      SparkColumnsToIndexSelector)
    val unmanaged = new QbeastContextImpl(
      config = SparkSession.active.sparkContext.getConf,
      keeper = keeper,
      indexedTableFactory = indexedTableFactory)
    QbeastContext.setUnmanaged(unmanaged)
    try {
      QbeastContext.keeper.isInstanceOf[Keeper]
    } finally {
      QbeastContext.unsetUnmanaged() shouldBe Some(unmanaged)
    }
  }

  it should "use the managed context after the unmanaged is unset" in withSpark { spark =>
    val keeper = LocalKeeper
    val indexedTableFactory = new IndexedTableFactoryImpl(
      keeper,
      SparkOTreeManager,
      SparkDeltaMetadataManager,
      RollupDataWriter,
      SparkRevisionFactory,
      SparkColumnsToIndexSelector)
    val unmanaged = new QbeastContextImpl(
      config = SparkSession.active.sparkContext.getConf,
      keeper = keeper,
      indexedTableFactory = indexedTableFactory)
    QbeastContext.setUnmanaged(unmanaged)
    QbeastContext.unsetUnmanaged() shouldBe Some(unmanaged)
    QbeastContext.keeper shouldBe LocalKeeper
  }

}
