/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.core.transform

import scala.util.hashing.MurmurHash3
import scala.util.Random

/**
 * A hash transformation of a coordinate
 * @param nullValue
 *   the value to use for null coordinates
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
   * @param other
   *   the new transformation created with statistics over the new data
   * @return
   *   true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(other: Transformation): Boolean = other match {
    case _: HashTransformation => false
    case _: EmptyTransformation => false
    case _ => true
  }

  override def merge(other: Transformation): Transformation = other match {
    case _: EmptyTransformation => this
    case _ => other
  }

}
