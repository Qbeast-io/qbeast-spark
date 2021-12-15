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
      .createWithDefault(100000)

  private[config] val min_partition_cube_size: ConfigEntry[Int] =
    ConfigBuilder("spark.qbeast.index.minPartitionCubeSize")
      .version("0.2.0")
      .intConf
      .createWithDefault(100)

  def DEFAULT_CUBE_SIZE: Int = QbeastContext.config
    .get(default_cube_size)

  def MIN_PARTITION_CUBE_SIZE: Int = QbeastContext.config
    .get(min_partition_cube_size)

}
