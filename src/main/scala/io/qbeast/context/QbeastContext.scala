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

import io.qbeast.core.keeper.Keeper
import io.qbeast.core.keeper.LocalKeeper
import io.qbeast.core.model._
import io.qbeast.spark.delta.writer.RollupDataWriter
import io.qbeast.spark.delta.DeltaMetadataManager
import io.qbeast.spark.index.SparkColumnsToIndexSelector
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.table.IndexedTableFactory
import io.qbeast.spark.table.IndexedTableFactoryImpl
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * Qbeast context provides access to internal mechanisms of the Qbeast index implementation.
 */
trait QbeastContext {

  /**
   * Returns the configuration.
   *
   * @return
   *   the configuration
   */
  def config: SparkConf

  /**
   * Returns the keeper.
   *
   * @return
   *   the keeper
   */
  def keeper: Keeper

  /**
   * Returns the IndexedTableFactory instance.
   *
   * @return
   *   the IndexedTableFactory instance
   */
  def indexedTableFactory: IndexedTableFactory
}

/**
 * Qbeast context companion object. <p> This object implements QbeastContext to be the single
 * instance used by the objects managed by Spark. By default it creates an internal managed Qbeast
 * context instance which is tired to the current Spark session and is released when the session
 * is closed. It is also possible to specify a custom unmanaged Qbeast context with #setUnmanaged
 * method which shadows the default one. Such an unmanaged context is not released with the Spark
 * session and should be used in testing scenarios only. To restore the default behavior use the
 * #unsetUnmanaged method.
 */
object QbeastContext
    extends QbeastContext
    with QbeastCoreContext[DataFrame, StructType, QbeastOptions, FileAction] {
  private var managedOption: Option[QbeastContext] = None
  private var unmanagedOption: Option[QbeastContext] = None

  // Override methods from QbeastContext

  override def config: SparkConf = current.config

  override def keeper: Keeper = current.keeper

  override def indexedTableFactory: IndexedTableFactory = current.indexedTableFactory

  // Override methods from QbeastCoreContext

  override def indexManager: IndexManager[DataFrame] = SparkOTreeManager

  override def metadataManager: MetadataManager[StructType, FileAction, QbeastOptions] =
    DeltaMetadataManager

  override def dataWriter: DataWriter[DataFrame, StructType, FileAction] =
    RollupDataWriter

  override def revisionBuilder: RevisionFactory[StructType, QbeastOptions] =
    SparkRevisionFactory

  override def columnSelector: ColumnsToIndexSelector[DataFrame] = SparkColumnsToIndexSelector

  /**
   * Sets the unmanaged context. The specified context will not be disposed automatically at the
   * end of the Spark session.
   *
   * @param context
   *   the unmanaged context to set
   */
  def setUnmanaged(context: QbeastContext): Unit = this.synchronized {
    unmanagedOption = Some(context)
  }

  /**
   * Unsets the unmanaged context.
   *
   * @return
   *   the unmanaged context
   */
  def unsetUnmanaged(): Option[QbeastContext] = this.synchronized {
    val result = unmanagedOption
    unmanagedOption = None
    result
  }

  private def current: QbeastContext = this.synchronized {
    unmanagedOption.getOrElse {
      managedOption.getOrElse {
        managedOption = Some(createManaged())
        SparkSession.active.sparkContext.addSparkListener(SparkListenerAdapter)
        managedOption.get
      }
    }
  }

  private def createManaged(): QbeastContext = {
    val config = SparkSession.active.sparkContext.getConf
    val keeper = createKeeper(config)
    val indexedTableFactory =
      createIndexedTableFactory(keeper)
    new QbeastContextImpl(config, keeper, indexedTableFactory)
  }

  private def createKeeper(config: SparkConf): Keeper = {
    val configKeeper = config.getAll.filter(_._1.startsWith("spark.qbeast.keeper")).toMap
    if (configKeeper.isEmpty) {
      LocalKeeper
    } else Keeper(configKeeper)
  }

  private def createIndexedTableFactory(keeper: Keeper): IndexedTableFactory =
    new IndexedTableFactoryImpl(
      keeper,
      indexManager,
      metadataManager,
      dataWriter,
      revisionBuilder,
      columnSelector)

  private def destroyManaged(): Unit = this.synchronized {
    managedOption.foreach(_.keeper.stop())
    managedOption = None
  }

  private object SparkListenerAdapter extends SparkListener {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit =
      destroyManaged()

  }

}

/**
 * Simple implementation of QbeastContext
 */
class QbeastContextImpl(
    val config: SparkConf,
    val keeper: Keeper,
    val indexedTableFactory: IndexedTableFactory)
    extends QbeastContext {}
