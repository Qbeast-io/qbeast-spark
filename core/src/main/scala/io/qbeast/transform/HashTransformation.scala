package io.qbeast.transform

import scala.util.hashing.MurmurHash3

case class HashTransformation() extends Transformation {

  override def transform(value: Any): Double = {
    value match {
      case s: String =>
        val randomInt = MurmurHash3.bytesHash(s.getBytes)

        (randomInt & 0x7fffffff).toDouble / Int.MaxValue
      case a: Array[Byte] =>
        val randomInt = MurmurHash3.bytesHash(a)

        (randomInt & 0x7fffffff).toDouble / Int.MaxValue
    }
  }

  /**
   * HashTransformation never changes
   * @param newTransformation the new transformation created with statistics over the new data
   *  @return true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean = false

  override def merge(other: Transformation): Transformation = this
}
