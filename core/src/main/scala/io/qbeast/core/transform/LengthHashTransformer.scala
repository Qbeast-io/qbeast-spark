package io.qbeast.core.transform

import io.qbeast.core.model.QDataType

object LengthHashTransformer extends TransformerType {
  override def transformerSimpleName: String = "length_hashing"

}

case class LengthHashTransformer(columnName: String, dataType: QDataType) extends Transformer {

  private val defaultEncodingLength = 11

  override protected def transformerType: TransformerType = LengthHashTransformer

  private def colLength = s"${columnName}_length"

  override def stats: ColumnStats =
    ColumnStats(statsNames = Seq(colLength), statsSqlPredicates = Seq.empty)

  override def makeTransformation(row: String => Any): Transformation = {

    // If no encoding length is found but want to use LengthHashTransformation
    // The default size for encoding would be 11 characters
    val encondingLength =
      try {
        val encondingLengthStats = row(colLength)
        if (encondingLengthStats == null) defaultEncodingLength
        else encondingLengthStats.asInstanceOf[Long].toInt
      } catch {
        case e: IllegalArgumentException
            if e.getMessage.contains(s"$colLength does not exist.") =>
          defaultEncodingLength
      }

    LengthHashTransformation(encodingLength = encondingLength)
  }

}
