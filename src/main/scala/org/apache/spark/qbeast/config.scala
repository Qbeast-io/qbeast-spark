/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.qbeast

import io.qbeast.context.QbeastContext
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

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

  private[config] val minFileSizeCompaction: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.compact.minFileSize")
      .version("0.2.0")
      .intConf
      .createWithDefault(1024 * 1024 * 1024)

  private[config] val maxFileSizeCompaction: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.compact.maxFileSize")
      .version("0.2.0")
      .intConf
      .createWithDefault(1024 * 1024 * 1024)

  def DEFAULT_NUMBER_OF_RETRIES: Int = QbeastContext.config
    .get(defaultNumberOfRetries)

  def DEFAULT_CUBE_SIZE: Int = QbeastContext.config
    .get(defaultCubeSize)

  def CUBE_WEIGHTS_BUFFER_CAPACITY: Long = QbeastContext.config
    .get(cubeWeightsBufferCapacity)

  def MIN_FILE_SIZE_COMPACTION: Int = QbeastContext.config.get(minFileSizeCompaction)

  def MAX_FILE_SIZE_COMPACTION: Int = QbeastContext.config.get(maxFileSizeCompaction)

}
