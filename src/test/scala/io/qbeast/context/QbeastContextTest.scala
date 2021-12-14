/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.context

import io.qbeast.core.keeper.{Keeper, LocalKeeper}
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.index.{SparkOTreeManager, SparkRevisionFactory}
import io.qbeast.spark.index.writer.SparkDataWriter
import io.qbeast.spark.table.IndexedTableFactoryImpl
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QbeastContextTest extends AnyFlatSpec with Matchers {

  it should "use the unmanaged context if provided" in {
    val keeper = LocalKeeper
    val indexedTableFactory = new IndexedTableFactoryImpl(
      keeper,
      SparkOTreeManager,
      SparkDeltaMetadataManager,
      SparkDataWriter,
      SparkRevisionFactory)
    val unmanaged = new QbeastContextImpl(
      config = SparkSession.active.sparkContext.getConf,
      keeper = keeper,
      indexedTableFactory = indexedTableFactory)
    QbeastContext.setUnmanaged(unmanaged)
    try {
      withSpark {
        QbeastContext.keeper.isInstanceOf[Keeper]
      }
    } finally {
      QbeastContext.unsetUnmanaged() shouldBe Some(unmanaged)
    }
  }

  it should "use the managed context after the unmanaged is unset" in {
    val keeper = LocalKeeper
    val indexedTableFactory = new IndexedTableFactoryImpl(
      keeper,
      SparkOTreeManager,
      SparkDeltaMetadataManager,
      SparkDataWriter,
      SparkRevisionFactory)
    val unmanaged = new QbeastContextImpl(
      config = SparkSession.active.sparkContext.getConf,
      keeper = keeper,
      indexedTableFactory = indexedTableFactory)
    QbeastContext.setUnmanaged(unmanaged)
    QbeastContext.unsetUnmanaged() shouldBe Some(unmanaged)
    withSpark {
      QbeastContext.keeper shouldBe LocalKeeper
    }
  }

  private def withSpark[T](code: => T): T = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    try {
      code
    } finally {
      session.close()
    }
  }

}
