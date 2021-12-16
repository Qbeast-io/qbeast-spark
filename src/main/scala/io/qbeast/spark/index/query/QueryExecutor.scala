/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

import io.qbeast.core.model.{QbeastFile, QbeastSnapshot}

/**
 * Executes a query against a Qbeast snapshot
 * @param querySpecBuilder the builder for the query specification
 */
class QueryExecutor(querySpecBuilder: QuerySpecBuilder, qbeastSnapshot: QbeastSnapshot) {

  /**
   * Executes the query given a previous matched files
   * @param previouslyMatchedFiles the sequence of files that have already been matched
   * @return the final sequence of files that match the query
   */
  def execute(previouslyMatchedFiles: Seq[QbeastFile]): Seq[QbeastFile] = {

    qbeastSnapshot.loadAllRevisions.flatMap { revision =>
      val querySpec = querySpecBuilder.build(revision)
      val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)
      val indexStatusExecutor = new QueryIndexStatusExecutor(querySpec, indexStatus)

      val matchingFiles = indexStatusExecutor.execute(previouslyMatchedFiles)
      matchingFiles
    }
  }

}
