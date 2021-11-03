/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.context

import com.typesafe.config.{Config, ConfigFactory}
import io.qbeast.keeper.{Keeper, LocalKeeper}
import io.qbeast.model.{
  IndexManager,
  MetadataManager,
  QTableIDProvider,
  QbeastCoreContext,
  QueryManager
}
import io.qbeast.spark.delta.SparkDeltaMetadataManager
import io.qbeast.spark.index.{OTreeAlgorithm, OTreeAlgorithmImpl, SparkIndexManager}
import io.qbeast.spark.internal.SparkQueryManager
import io.qbeast.spark.table.{IndexedTableFactory, IndexedTableFactoryImpl, SparkQTableIdProvider}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession

/**
 * Qbeast context provides access to internal mechanisms of
 * the Qbeast index implementation.
 */
trait QbeastContext {

  /**
   * Returns the configuration.
   *
   * @return the configuration
   */
  def config: Config

  /**
   * Returns the keeper.
   *
   * @return the keeper
   */
  def keeper: Keeper

  /**
   * Returns the OTreeAlgorithm instance.
   *
   * @return the OTreeAlgorithm instance
   */
  def oTreeAlgorithm: OTreeAlgorithm

  /**
   * Returns the IndexedTableFactory instance.
   *
   * @return the IndexedTableFactory instance
   */
  def indexedTableFactory: IndexedTableFactory
}

/**
 * Qbeast context companion object.
 * <p>
 * This object implements QbeastContext to be the single instance
 * used by the objects managed by Spark. By default it creates an internal
 * managed Qbeast context instance which is tired to the current Spark session
 * and is released when the session is closed. It is also possible to specify
 * a custom unmanaged Qbeast context with #setUnmanaged method which shadows
 * the default one. Such an unmanaged context is not released with the Spark
 * session and should be used in testing scenarios only. To restore the default
 * behavior use the #unsetUnmanaged method.
 */
object QbeastContext extends QbeastContext with QbeastCoreContext {
  private var managedOption: Option[QbeastContext] = None
  private var unmanagedOption: Option[QbeastContext] = None

  // Override methods from QbeastCoreContext
  override def queryManager[Q, T]: QueryManager[Q, T] =
    new SparkQueryManager

  override def indexManager[T]: IndexManager[T] = new SparkIndexManager

  override def metadataManager: MetadataManager = new SparkDeltaMetadataManager

  override def qtableIDProvider: QTableIDProvider = new SparkQTableIdProvider

  // Override methods from QbeastContext trait
  override def config: Config = current.config

  override def keeper: Keeper = LocalKeeper

  override def oTreeAlgorithm: OTreeAlgorithm = current.oTreeAlgorithm

  override def indexedTableFactory: IndexedTableFactory = current.indexedTableFactory

  /**
   * Sets the unmanaged context. The specified context will not
   * be disposed automatically at the end of the Spark session.
   *
   * @param context the unmanaged context to set
   */
  def setUnmanaged(context: QbeastContext): Unit = this.synchronized {
    unmanagedOption = Some(context)
  }

  /**
   * Unsets the unmanaged context.
   *
   * @return the unmanaged context
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
    val config = ConfigFactory.load()
    val keeper = createKeeper(config)
    val oTreeAlgorithm = createOTreeAlgorithm(config)
    val indexedTableFactory = createIndexedTableFactory(keeper, oTreeAlgorithm)
    new QbeastContextImpl(config, keeper, oTreeAlgorithm, indexedTableFactory)
  }

  private def createKeeper(config: Config): Keeper = Keeper(config)

  private def createOTreeAlgorithm(config: Config): OTreeAlgorithm = {
    val desiredCubeSize = config.getInt("qbeast.index.size")
    new OTreeAlgorithmImpl(desiredCubeSize)
  }

  private def createIndexedTableFactory(
      keeper: Keeper,
      oTreeAlgorithm: OTreeAlgorithm): IndexedTableFactory =
    new IndexedTableFactoryImpl(keeper, oTreeAlgorithm)

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
    val config: Config,
    val keeper: Keeper,
    val oTreeAlgorithm: OTreeAlgorithm,
    val indexedTableFactory: IndexedTableFactory)
    extends QbeastContext {}
