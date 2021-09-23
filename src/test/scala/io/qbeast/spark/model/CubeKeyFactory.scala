/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.model

object CubeKeyFactory extends Serializable {

  @inline
  def createCubeKey(dime: Point, times: Int)(implicit
      dimensionalContext: DimensionalContext): String =
    CubeKeyFactoryJava.createCubeKey(
      dime.coordinates.toArray,
      times,
      dimensionalContext.characterOffset)

  def old_createCubeKey(dime: Point, times: Int)(implicit
      dimensionalContext: DimensionalContext): String = {

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
        string + (id + dimensionalContext.characterOffset).toChar
      }

  }

}
