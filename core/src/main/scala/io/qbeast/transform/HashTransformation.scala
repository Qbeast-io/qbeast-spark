package io.qbeast.transform

import scala.util.hashing.MurmurHash3

case class HashTransformation() extends Transformation {

  override def transform(value: Any): Double = {
    value match {
      case s: String => MurmurHash3.bytesHash(s.getBytes).toDouble

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
