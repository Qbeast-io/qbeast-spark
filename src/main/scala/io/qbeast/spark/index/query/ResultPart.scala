/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index.query

/**
 * Result part represents the query result data to be read from a single table
 * file.
 *
 * @param filePath the file path
 * @param fileLength the file length in bytes
 * @param fileModificationTime the file last modification timestamp
 * @param from the first row to read the data
 * @param to the last row (exclusive) to read the data
 */
case class ResultPart(
    val filePath: String,
    val fileLength: Long,
    val fileModificationTime: Long,
    val from: Long,
    val to: Long)
