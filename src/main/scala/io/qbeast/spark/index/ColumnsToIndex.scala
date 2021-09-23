/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

/**
 * Utility object to work with the columns to index.
 */
object ColumnsToIndex {
  private val separator = ","

  /**
   * Decodes columns to index from a given string.
   *
   * @param string the string to decode
   * @return the decoded columns to index
   */
  def decode(string: String): Seq[String] = string.split(separator).toSeq

  /**
   * Encodes given columns to index to a single string.
   *
   * @param columnsToIndex the columns to index
   * @return the encoded columns to index
   */
  def encode(columnsToIndex: Seq[String]): String = columnsToIndex.mkString(separator)

  /**
   * Returns whether two given sets of columns to index are the same,
   * i.e. equals modulo some permutation.
   *
   * @param columnsToIndex the columns to index
   * @param otherColumnsToIndex the other columns to index
   * @return the seta are the same
   */
  def areSame(columnsToIndex: Seq[String], otherColumnsToIndex: Seq[String]): Boolean =
    columnsToIndex.toSet == otherColumnsToIndex.toSet

}
