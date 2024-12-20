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

import io.qbeast.core.model._
import io.qbeast.spark.delta.DeltaMetadataManager
import io.qbeast.spark.delta.DeltaRollupDataWriter
import io.qbeast.spark.index.SparkColumnsToIndexSelector
import io.qbeast.spark.index.SparkOTreeManager
import io.qbeast.spark.index.SparkRevisionFactory
import io.qbeast.table.IndexedTableFactory
import io.qbeast.table.IndexedTableFactoryImpl
import org.apache.spark.qbeast.config.DEFAULT_TABLE_FORMAT
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerApplicationEnd
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
object QbeastContext extends QbeastContext with QbeastCoreContext {
  private var managedOption: Option[QbeastContext] = None
  private var unmanagedOption: Option[QbeastContext] = None

  // Override methods from QbeastContext

  override def config: SparkConf = current.config

  override def indexedTableFactory: IndexedTableFactory = current.indexedTableFactory

  // Override methods from QbeastCoreContext

  override def indexManager: IndexManager = SparkOTreeManager

  override def metadataManager: MetadataManager = {
    DEFAULT_TABLE_FORMAT match {
      case "delta" => DeltaMetadataManager
      case _ =>
        throw new IllegalArgumentException(
          s"MetadataManager for table format $DEFAULT_TABLE_FORMAT not found")
    }
  }

  override def dataWriter: DataWriter = {
    DEFAULT_TABLE_FORMAT match {
      case "delta" => DeltaRollupDataWriter
      case _ =>
        throw new IllegalArgumentException(
          s"RollupDataWriter for table format $DEFAULT_TABLE_FORMAT not found")
    }
  }

  override def revisionBuilder: RevisionFactory = SparkRevisionFactory

  override def columnSelector: ColumnsToIndexSelector = SparkColumnsToIndexSelector

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
    val indexedTableFactory = createIndexedTableFactory()
    new QbeastContextImpl(config, indexedTableFactory)
  }

  private def createIndexedTableFactory(): IndexedTableFactory =
    new IndexedTableFactoryImpl(
      indexManager,
      metadataManager,
      dataWriter,
      revisionBuilder,
      columnSelector)

  private def destroyManaged(): Unit = this.synchronized {
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
class QbeastContextImpl(val config: SparkConf, val indexedTableFactory: IndexedTableFactory)
    extends QbeastContext {}
