package io.qbeast.core.transform

import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
 * A hash transformation of a coordinate
 * @param nullValue the value to use for null coordinates
 */
case class HashTransformation(nullValue: Any = Random.nextInt()) extends Transformation {

  override def transform(value: Any): Double = {
    val v = if (value == null) nullValue else value
    val hash = v match {
      case s: String =>
        MurmurHash3.bytesHash(s.getBytes)
      case n: Number =>
        MurmurHash3.bytesHash(n.toString.getBytes)
      case a: Array[Byte] =>
        MurmurHash3.bytesHash(a)
    }
    (hash & 0x7fffffff).toDouble / Int.MaxValue
  }

  /**
   * HashTransformation never changes
   * @param newTransformation the new transformation created with statistics over the new data
   *  @return true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean = false

  override def merge(other: Transformation): Transformation = this
}
