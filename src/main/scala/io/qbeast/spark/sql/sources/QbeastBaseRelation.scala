/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

/**
 * Implementation of BaseRelation which wraps the
 * original relation created by Delta Lakes.
 *
 * @param delta the wrapped instance created by Delta Lakes
 */
case class QbeastBaseRelation(delta: BaseRelation, columnsToIndex: Seq[String])
    extends BaseRelation {
  override def sqlContext: SQLContext = delta.sqlContext

  override def schema: StructType = delta.schema

}
