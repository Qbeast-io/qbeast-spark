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
package io.qbeast.core.transform

import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.model.QDataType
import io.qbeast.core.model.StringDataType
import org.apache.spark.sql.AnalysisExceptionFactory

object CDFQuantilesTransformer extends TransformerType {
  override def transformerSimpleName: String = "quantiles"

  /**
   * Returns the CDFQuantilesTransformer that would compute the Quantiles for the defined
   * columnName and dataType and initialize a new instance of CDFQuantilesTransformation
   *
   * @param columnName
   *   the column name
   * @param dataType
   *   the data type
   * @return
   *   the CDFFQuantilesTransformer, which can be of type CDFNumericQuantilesTransformer or
   *   CDFStringQuantilesTransformer
   */
  override def apply(columnName: String, dataType: QDataType): CDFQuantilesTransformer = {
    dataType match {
      case ord: OrderedDataType => CDFNumericQuantilesTransformer(columnName, ord)
      case StringDataType => CDFStringQuantilesTransformer(columnName)
      case _ =>
        throw AnalysisExceptionFactory.create(
          s"CDFQuantilesTransformer can only be applied to OrderedDataType columns " +
            s"or StringDataType columns. Column $columnName is of type $dataType")
    }
  }

}

/**
 * A transformer that calculates the CDF quantiles
 */
trait CDFQuantilesTransformer extends Transformer {

  val columnQuantile: String = s"${columnName}_quantile"

  val defaultQuantiles: IndexedSeq[Any]

  override protected def transformerType: TransformerType = CDFQuantilesTransformer

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats = {
    ColumnStats(s"${columnName}_quantiles" :: Nil, Nil)
  }

}
