/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.delta.writer

import io.qbeast.IISeq
import io.qbeast.core.model.IndexFile

/**
 * Strategy to compact given index files.
 */
private[writer] trait CompactStrategy {

  /**
   * Compacts given index files and returns the compacted index files.
   *
   * @param indexFiles the index files to compact.
   * @return the new compact index files and the old index files to remove
   */
  def compact(indexFiles: IISeq[IndexFile]): (IISeq[IndexFile], IISeq[IndexFile])

}
