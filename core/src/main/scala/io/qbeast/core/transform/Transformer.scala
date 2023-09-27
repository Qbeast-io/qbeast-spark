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
    Seq(LinearTransformer, HashTransformer, StringHistogramTransformer)
      .map(a => (a.transformerSimpleName, a))
      .toMap

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
      case StringDataType => StringHistogramTransformer.transformerSimpleName
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
   * Returns the name of the column
   * @return
   */
  def columnName: String

  /**
   * Returns the stats
   * @return
   */
  def stats: ColumnStats

  /**
   * Returns the Transformation given a row representation of the values
   * @param row the values
   * @return the transformation
   */
  def makeTransformation(row: String => Any): Transformation

  /**
   * Returns the new Transformation if the space has changed
   * @param currentTransformation the current transformation
   * @param row the row containing the new space values
   * @return an optional new transformation
   */
  def maybeUpdateTransformation(
      currentTransformation: Transformation,
      row: Map[String, Any]): Option[Transformation] = {
    val newDataTransformation = makeTransformation(row)
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
object NoColumnStats extends ColumnStats(Nil, Nil)

/**
 * Stores the stats of the column
 * @param statsNames the names of the stats
 * @param statsSqlPredicates the stats column predicates
 */
case class ColumnStats(statsNames: Seq[String], statsSqlPredicates: Seq[String])
    extends Serializable {

  /**
   * Gets the values of the stats
   * @param row the row of values
   * @return the stats values
   */
  def getValues(row: Map[String, Any]): Seq[Any] = statsNames.map(column => row(column))
}
