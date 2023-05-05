package io.qbeast.core.transform

import scala.util.Random
import scala.util.hashing.MurmurHash3

case class LengthHashTransformation(nullValue: Any = Random.nextInt(), encodingLength: Int)
    extends Transformation {

  override def transform(value: Any): Double = {
    val v = if (value == null) nullValue else value
    val string = v match {
      case s: String => s
      case n: Number => n.toString
      case a: Array[Byte] => a.mkString
    }

    val newString = string.padTo(encodingLength, 'a')
    val hash = MurmurHash3.bytesHash(newString.getBytes)
    (hash & 0x7fffffff).toDouble / Int.MaxValue
  }

  override def isSupersededBy(newTransformation: Transformation): Boolean = false

  override def merge(other: Transformation): Transformation = this
}
