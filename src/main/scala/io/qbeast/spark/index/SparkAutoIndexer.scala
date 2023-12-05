package io.qbeast.spark.index

import io.qbeast.core.model.AutoIndexer
import org.apache.spark.sql.DataFrame

object SparkAutoIndexer extends AutoIndexer[DataFrame] with Serializable {

  override def simpleName: String = "SparkAutoIndexer"

  override val MAX_COLUMNS_TO_INDEX: Int = 32

  override def chooseColumnsToIndex(data: DataFrame): Seq[String] = {
    data.schema.fields.take(MAX_COLUMNS_TO_INDEX).map(_.name) // TODO
  }

}
