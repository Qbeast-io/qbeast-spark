/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

/**
 * Query file represents the query results to be read from a single file, the
 * specified ranges defines what data meets the query requirements.
 *
 * @param file the file to read the results from
 * @param ranges the rages of elements to be read from the file
 */
final case class QueryFile(file: File, ranges: Seq[RowRange]) {}
