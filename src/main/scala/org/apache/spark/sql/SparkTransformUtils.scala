/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.connector.expressions.{
  FieldReference,
  IdentityTransform,
  Literal,
  NamedReference,
  Transform
}
import org.apache.spark.sql.types.IntegerType

import scala.collection.mutable

object SparkTransformUtils {

  /**
   * Copy of V2 convertTransforms
   * @param partitions
   * @return
   */
  def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, bucketCols, sortCols) =>
        bucketSpec = Some(
          BucketSpec(
            numBuckets,
            bucketCols.map(_.fieldNames.head),
            sortCols.map(_.fieldNames.head)))

      case _ =>
        throw AnalysisExceptionFactory.create(
          "Operation not supported, non partition based transformation")
    }

    (identityCols, bucketSpec)
  }

}

object BucketTransform {

  def unapply(transform: Transform): Option[(Int, Seq[NamedReference], Seq[NamedReference])] = {
    val arguments = transform.arguments()
    if (transform.name() == "sorted_bucket") {
      var posOfLit: Int = -1
      var numOfBucket: Int = -1
      arguments.zipWithIndex.foreach {
        case (literal: Literal[_], i) if literal.dataType() == IntegerType =>
          numOfBucket = literal.value().asInstanceOf[Integer]
          posOfLit = i
        case _ =>
      }
      Some(
        (
          numOfBucket,
          arguments.take(posOfLit).map(_.asInstanceOf[NamedReference]),
          arguments.drop(posOfLit + 1).map(_.asInstanceOf[NamedReference])))
    } else if (transform.name() == "bucket") {
      val numOfBucket = arguments(0) match {
        case literal: Literal[_] if literal.dataType() == IntegerType =>
          literal.value().asInstanceOf[Integer]
        case _ => throw new IllegalStateException("invalid bucket transform")
      }
      Some(
        (
          numOfBucket,
          arguments.drop(1).map(_.asInstanceOf[NamedReference]),
          Seq.empty[FieldReference]))
    } else {
      None
    }
  }

}
