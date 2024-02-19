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
package io.qbeast.spark.internal.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, HashExpression, Murmur3HashFunction}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * Qbeast hash expression based on Murmur3Hash algorithm
 *
 * @param children Sequence of expressions to hash
 * @param seed Seed for the Hash Expression
 */
case class QbeastMurmur3Hash(children: Seq[Expression], seed: Int) extends HashExpression[Int] {
  def this(arguments: Seq[Expression]) = this(arguments, 42)

  override def dataType: DataType = IntegerType

  override def prettyName: String = "qbeast_hash"

  override protected def hasherClassName: String = classOf[Murmur3_x86_32].getName

  override protected def computeHash(value: Any, dataType: DataType, seed: Int): Int = {
    Murmur3HashFunction.hash(value, dataType, seed).toInt
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)

}
