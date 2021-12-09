/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal

import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.Column

/**
 * Qbeast Functions for Spark Columns
 */
object QbeastFunctions {

  /**
   * QbeastMurmur3Hash function
   * @param cols the columns to hash
   * @return the resultant column
   */
  def qbeastHash(cols: Column*): Column =
    new Column(new QbeastMurmur3Hash(cols.map(_.expr)))

}
