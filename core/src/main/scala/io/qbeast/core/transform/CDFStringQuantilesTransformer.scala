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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.AnalysisExceptionFactory

/**
 * CDF Quantile Transformer for Strings
 *
 * @param columnName
 *   the name of the column
 */
@Experimental
case class CDFStringQuantilesTransformer(columnName: String) extends CDFQuantilesTransformer {

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row
   *   the values
   * @return
   *   the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {
    row(columnTransformerName) match {
      case null => EmptyTransformation()
      case q: Seq[_] =>
        val quantiles = q.map(_.toString).toIndexedSeq
        CDFStringQuantilesTransformation(quantiles)
      case _ =>
        throw AnalysisExceptionFactory.create(
          s"Quantiles for column $columnName should be of type Array[String]")
    }

  }

}
