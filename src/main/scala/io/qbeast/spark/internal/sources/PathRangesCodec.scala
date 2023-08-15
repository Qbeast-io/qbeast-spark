/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources

import io.qbeast.core.model.RowRange
import scala.util.Try
import scala.util.Success

/**
 * Codec to encode and decode paths with ranges of rows.
 */
object PathRangesCodec {

  /**
   * Encodes given path and ranges of rows.
   *
   * @param path the path
   * @param ranges the ranges
   */
  def encode(path: String, ranges: Seq[RowRange]): String = {
    if (ranges.isEmpty) {
      return path
    }
    val builder = StringBuilder.newBuilder
    builder ++= path
    builder += '#'
    ranges.foreach { case RowRange(from, to) =>
      builder.append(from).append('-').append(to).append(',')
    }
    builder.deleteCharAt(builder.length - 1)
    builder.result()
  }

  /**
   * Decodes a given string extracting the path and ranges of rows. Invalid
   * range expressions are ignored.
   *
   * @param source the string to decode
   * @return the path and the ranges of rows
   */
  def decode(source: String): (String, Seq[RowRange]) = {
    val index = source.lastIndexOf('#')
    if (index < 0) {
      return (source, Seq.empty)
    }
    val path = source.substring(0, index)
    val ranges = source
      .substring(index + 1)
      .split(',')
      .map(_.split('-'))
      .map(_.map(s => Try(s.toLong)))
      .collect { case Array(Success(from), Success(to)) => RowRange(from, to) }
    (path, ranges)
  }

}
