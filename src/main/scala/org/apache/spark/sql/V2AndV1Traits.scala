package org.apache.spark.sql

import org.apache.spark.sql.connector.catalog.{Table, V1Table}

object V1TableQbeast {

  def unapply(table: Table): Option[V1Table] = table match {
    case v1T @ V1Table(_) => Some(v1T)
    case _ => None
  }

}
