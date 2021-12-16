/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.qbeast.core.model.{OrderedDataType, QDataType, StringDataType}

import java.util.Locale

/**
 * Transformer object that choose the right transformation function
 */
object Transformer {

  private val transformersRegistry: Map[String, TransformerType] =
    Seq(LinearTransformer, HashTransformer).map(a => (a.transformerSimpleName, a)).toMap

  /**
   * Returns the transformer for the given column and type of transformer
   * @param transformerTypeName the name of the transformer type: could be hashing or linear
   * @param columnName the name of the column
   * @param dataType the type of the data
   * @return the Transformer
   */
  def apply(transformerTypeName: String, columnName: String, dataType: QDataType): Transformer = {

    val tt = transformerTypeName.toLowerCase(Locale.ROOT)
    transformersRegistry(tt)(columnName, dataType)
  }

  /**
   * Returns the transformer for a given column
   * @param columnName the name of the column
   * @param dataType the type of the data
   * @return the Transformer
   */
  def apply(columnName: String, dataType: QDataType): Transformer = {
    getDefaultTransformerForType(dataType)(columnName, dataType)
  }

  /**
   * Returns the transformer type for a given data type
   * @param dataType the type of the data
   * @return the transformer type: could be hashing or linear
   */
  def getDefaultTransformerForType(dataType: QDataType): TransformerType = transformersRegistry {
    dataType match {
      case _: OrderedDataType => LinearTransformer.transformerSimpleName
      case StringDataType => HashTransformer.transformerSimpleName
      case _ => throw new RuntimeException(s"There's not default transformer for $dataType")
    }

  }

}

/**
 * Transformer type
 */
private[transform] trait TransformerType {
  def transformerSimpleName: String

  def apply(columnName: String, dataType: QDataType): Transformer
}

/**
 * Transformer interface
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.CLASS,
  include = JsonTypeInfo.As.PROPERTY,
  property = "className")
trait Transformer extends Serializable {

  protected def transformerType: TransformerType

  /**
   * Returns the data type of the transformer
   * @return
   */
  def dataType: QDataType

  /**
   * Returns the name of the column
   * @return
   */
  def columnName: String

  /**
   * Returns the Transformation given a row representation of the values
   * @param columnStats the column stats for the transformation
   * @return the transformation
   */
  def makeTransformation(columnStats: ColumnStats): Transformation

  /**
   * Returns the new Transformation if the space has changed
   * @param currentTransformation the current transformation
   * @param columnStats the column stats for the transformation
   * @return an optional new transformation
   */
  def maybeUpdateTransformation(
      currentTransformation: Transformation,
      columnStats: ColumnStats): Option[Transformation] = {
    val newDataTransformation = makeTransformation(columnStats)
    if (currentTransformation.isSupersededBy(newDataTransformation)) {
      Some(currentTransformation.merge(newDataTransformation))
    } else {
      None
    }
  }

  def spec: String = s"$columnName:${transformerType.transformerSimpleName}"

}

/**
 * Empty ColumnStats
 */
object NoColumnStats extends ColumnStats(Nil, Nil, 0, 0.0, Nil)

/**
 * Stores the stats of the column
 * @param min    min value of the column
 * @param max    max value of the column
 * @param count  number of values in the column
 * @param stddev standard deviation of the column
 * @param mean   mean of the column
 */
case class ColumnStats(min: Any, max: Any, count: Long, stddev: Double, mean: Any)
    extends Serializable {}
