/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.qbeast

import io.qbeast.context.QbeastContext
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

package object config {

  private[config] val default_cube_size: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.defaultCubeSize")
      .version("0.2.0")
      .intConf
      .createWithDefault(5000000)

  private[config] val cube_weights_buffer_capacity: ConfigEntry[Long] =
    ConfigBuilder("spark.qbeast.index.cubeWeightsBufferCapacity")
      .version("0.2.0")
      .longConf
      .createWithDefault(100000L)

  private[config] val default_number_of_retries: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.numberOfRetries")
      .version("0.2.0")
      .intConf
      .createWithDefault(2)

  def DEFAULT_NUMBER_OF_RETRIES: Int = QbeastContext.config
    .get(default_number_of_retries)

  def DEFAULT_CUBE_SIZE: Int = QbeastContext.config
    .get(default_cube_size)

  def CUBE_WEIGHTS_BUFFER_CAPACITY: Long = QbeastContext.config
    .get(cube_weights_buffer_capacity)

}
