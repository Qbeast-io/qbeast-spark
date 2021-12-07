/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

object CubeKeyFactory extends Serializable {

  def characterOffset(numberOfDimensions: Int): Int = {
    numberOfDimensions match {
      case a if a <= 3 => '0'
      case a if a <= 5 => 'A'
      case 6 => '0'
      case _ => 0 // in this case we can't print
    }
  }

  @inline
  def createCubeKey(dime: Point, times: Int): String =
    CubeKeyFactoryJava.createCubeKey(
      dime.coordinates.toArray,
      times,
      characterOffset(dime.coordinates.size))

  def old_createCubeKey(dime: Point, times: Int): String = {

    val mul = BigDecimal(BigInt(1) << times)
    val rounded = dime.coordinates.map(a => (a * mul).toBigInt())
    (times - 1)
      .to(0, -1)
      .map { t =>
        val mask = BigInt(1) << t
        rounded.zipWithIndex
          .map { case (value, index) =>
            ((value & mask) >> t) << index
          }
          .reduce(_ | _)
          .toByte
      }
      .foldLeft("") { case (string, id) =>
        string + (id + characterOffset(dime.coordinates.size)).toChar
      }

  }

}
