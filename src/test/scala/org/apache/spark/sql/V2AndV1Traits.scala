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
package org.apache.spark.sql

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.catalog.V2TableWithV1Fallback

trait V2toV1Fallback extends V2TableWithV1Fallback {}

object V1TableQbeast {

  def unapply(table: Table): Option[V1Table] = table match {
    case v1T @ V1Table(_) => Some(v1T)
    case _ => None
  }

}
