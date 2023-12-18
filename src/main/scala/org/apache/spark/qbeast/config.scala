/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.qbeast

import io.qbeast.context.QbeastContext
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.internal.config.OptionalConfigEntry

package object config {

  private[config] val defaultCubeSize: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.defaultCubeSize")
      .version("0.2.0")
      .intConf
      .createWithDefault(5000000)

  private[config] val cubeWeightsBufferCapacity: ConfigEntry[Long] =
    ConfigBuilder("spark.qbeast.index.cubeWeightsBufferCapacity")
      .version("0.2.0")
      .longConf
      .createWithDefault(100000L)

  private[config] val defaultNumberOfRetries: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.numberOfRetries")
      .version("0.2.0")
      .intConf
      .createWithDefault(2)

  private[config] val minCompactionFileSizeInBytes: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.compact.minFileSizeInBytes")
      .version("0.2.0")
      .intConf
      .createWithDefault(1024 * 1024 * 1024)

  private[config] val maxCompactionFileSizeInBytes: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.compact.maxFileSizeInBytes")
      .version("0.2.0")
      .intConf
      .createWithDefault(1024 * 1024 * 1024)

  private[config] val stagingSizeInBytes: OptionalConfigEntry[Long] =
    ConfigBuilder("spark.qbeast.index.stagingSizeInBytes")
      .version("0.2.0")
      .longConf
      .createOptional

  private[config] val columnsToIndexSelectorEnabled: ConfigEntry[Boolean] =
    ConfigBuilder("spark.qbeast.index.columnsToIndex.auto")
      .version("0.2.0")
      .booleanConf
      .createWithDefault(false)

  private[config] val maxNumColumnsToIndex: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.columnsToIndex.auto.max")
      .version("0.2.0")
      .intConf
      .createWithDefault(3)

  def DEFAULT_NUMBER_OF_RETRIES: Int = QbeastContext.config
    .get(defaultNumberOfRetries)

  def DEFAULT_CUBE_SIZE: Int = QbeastContext.config
    .get(defaultCubeSize)

  def CUBE_WEIGHTS_BUFFER_CAPACITY: Long = QbeastContext.config
    .get(cubeWeightsBufferCapacity)

  def MIN_COMPACTION_FILE_SIZE_IN_BYTES: Int =
    QbeastContext.config.get(minCompactionFileSizeInBytes)

  def MAX_COMPACTION_FILE_SIZE_IN_BYTES: Int =
    QbeastContext.config.get(maxCompactionFileSizeInBytes)

  def STAGING_SIZE_IN_BYTES: Option[Long] = QbeastContext.config.get(stagingSizeInBytes)

  def COLUMN_SELECTOR_ENABLED: Boolean = QbeastContext.config.get(columnsToIndexSelectorEnabled)

  def MAX_NUM_COLUMNS_TO_INDEX: Int = QbeastContext.config.get(maxNumColumnsToIndex)

}
