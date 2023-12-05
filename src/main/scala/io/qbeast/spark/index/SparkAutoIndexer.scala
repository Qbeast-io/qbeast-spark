/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.AutoIndexer
import org.apache.spark.sql.DataFrame

object SparkAutoIndexer extends AutoIndexer[DataFrame] with Serializable {

  override val MAX_COLUMNS_TO_INDEX: Option[Int] = None

  override def chooseColumnsToIndex(data: DataFrame): Seq[String] = {
    val numColumnsToIndex = MAX_COLUMNS_TO_INDEX.getOrElse(data.schema.fields.length)
    data.schema.fields.take(numColumnsToIndex).map(_.name) // TODO
  }

}
