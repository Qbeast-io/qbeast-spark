package io.qbeast.spark.index.query

import io.qbeast.TestClasses.T2
import io.qbeast.core.model.{IntegerDataType, QTableID, QuerySpaceFromTo, Revision, WeightRange}
import io.qbeast.core.transform.{LinearTransformation, Transformer}
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{
  And,
  Expression,
  GreaterThanOrEqual,
  LessThan,
  Literal
}
import org.scalatest.PrivateMethodTester
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

trait QueryTestSpec extends AnyFlatSpec with Matchers with PrivateMethodTester {

  val privateFrom: PrivateMethod[QuerySpaceFromTo] = PrivateMethod[QuerySpaceFromTo]('from)
  val privateTo: PrivateMethod[QuerySpaceFromTo] = PrivateMethod[QuerySpaceFromTo]('to)

  def createDF(size: Int, spark: SparkSession): Dataset[T2] = {
    import spark.implicits._

    0.to(size)
      .map(i => T2(i, i.toDouble))
      .toDF()
      .as[T2]

  }

  def weightFilters(weightRange: WeightRange): Expression = {
    val qbeast_hash = new QbeastMurmur3Hash(Seq(new Column("a").expr, new Column("c").expr))
    val lessThan = LessThan(qbeast_hash, Literal(weightRange.to.value))
    val greaterThanOrEqual = GreaterThanOrEqual(qbeast_hash, Literal(weightRange.from.value))
    And(lessThan, greaterThanOrEqual)
  }

  def createRevision(minVal: Int = Int.MinValue, maxVal: Int = Int.MaxValue): Revision = {
    val transformations =
      Seq(LinearTransformation(minVal, maxVal, IntegerDataType)).toIndexedSeq
    val columnTransformers = Seq(Transformer("linear", "id", IntegerDataType)).toIndexedSeq

    Revision(
      1,
      System.currentTimeMillis(),
      QTableID("test"),
      100,
      columnTransformers,
      transformations)
  }

}
