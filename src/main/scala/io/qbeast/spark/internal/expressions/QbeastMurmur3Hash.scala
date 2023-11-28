/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, HashExpression, Murmur3HashFunction}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * Qbeast hash expression based on Murmur3Hash algorithm
 *
 * @param children
 *   Sequence of expressions to hash
 * @param seed
 *   Seed for the Hash Expression
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
