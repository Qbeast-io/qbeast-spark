/*
 * Copyright 2021 Qbeast Analytics, S.L.
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
