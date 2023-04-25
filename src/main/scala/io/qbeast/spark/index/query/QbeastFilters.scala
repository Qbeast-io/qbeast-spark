/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import org.apache.spark.sql.catalyst.expressions.Expression

case class QbeastFilters(weightFilters: Seq[Expression], queryFilters: Seq[Expression])
