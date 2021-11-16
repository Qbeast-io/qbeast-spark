/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.Column

object QbeastFunctions {

  def qbeastHash(cols: Column*): Column =
    new Column(new QbeastMurmur3Hash(cols.map(_.expr)))

}
