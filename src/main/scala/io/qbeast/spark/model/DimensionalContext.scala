/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.model

object DimensionalContext {

  def apply(numberOfDimensions: Int): DimensionalContext = {

    val charOff = numberOfDimensions match {
      case a if a <= 3 => '0'
      case a if a <= 5 => 'A'
      case 6 => '0'
      case _ => 0 // in this case we can't print
    }
    val dimSpace = 1 << numberOfDimensions
    new DimensionalContext(
      Math.log(numberOfDimensions),
      numberOfDimensions,
      dimSpace,
      charOff,
      0.until(dimSpace).map(a => (a + charOff).toChar))
  }

}

/**
 * @param dimensionLog       the ln(#dimension) to use for logarithms  in dimension space
 * @param numberOfDimensions the number of dimensions
 * @param dimSpace           the dimensional fan-out 2 pow (#dimensions)
 * @param characterOffset    the offset to remove from the dimension identifier
 */

case class DimensionalContext(
    dimensionLog: DimensionLog,
    numberOfDimensions: Int,
    dimSpace: Int,
    characterOffset: Int,
    alphabet: Seq[Char])
