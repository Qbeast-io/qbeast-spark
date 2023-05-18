/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import org.apache.spark.sql.catalyst.expressions.Expression
import io.qbeast.core.model.Revision
import io.qbeast.core.model.QuerySpace

/**
 * Factory fro creating query spaces for different index revisions.
 */
private[query] class QuerySpacesFactory(filters: Seq[Expression]) {

  /**
   * Creates new query spaces organized like CNF, i.e. the inner sequences are
   * interpreted as union of spaces, and the outer sequence is intersection of
   * the inner ones.
   */
  def newQuerySpaces(revision: Revision): Seq[Seq[QuerySpace]] = {
    throw new UnsupportedOperationException("Not implemented yet.")
  }

}
