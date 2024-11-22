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

import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.model.QDataType
import io.qbeast.core.model.StringDataType

import java.util.Locale

/**
 * Transformer object that choose the right transformation function
 */
object Transformer {

  private val transformersRegistry: Map[String, TransformerType] =
    Seq(LinearTransformer, HashTransformer, CDFQuantilesTransformer, HistogramTransformer)
      .map(a => (a.transformerSimpleName, a))
      .toMap

  /**
   * Returns the transformer for the given column and type of transformer
   * @param transformerTypeName
   *   the name of the transformer type: could be hashing or linear
   * @param columnName
   *   the name of the column
   * @param dataType
   *   the type of the data
   * @return
   *   the Transformer
   */
  def apply(transformerTypeName: String, columnName: String, dataType: QDataType): Transformer = {

    val tt = transformerTypeName.toLowerCase(Locale.ROOT)
    transformersRegistry(tt)(columnName, dataType)
  }

  /**
   * Returns the transformer for a given column
   * @param columnName
   *   the name of the column
   * @param dataType
   *   the type of the data
   * @return
   *   the Transformer
   */
  def apply(columnName: String, dataType: QDataType): Transformer = {
    getDefaultTransformerForType(dataType)(columnName, dataType)
  }

  /**
   * Returns the transformer type for a given data type
   * @param dataType
   *   the type of the data
   * @return
   *   the transformer type: could be hashing or linear
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
   * @param row
   *   the values
   * @return
   *   the transformation
   */
  def makeTransformation(row: String => Any): Transformation

  /**
   * Returns the new Transformation if the space has changed
   * @param currentTransformation
   *   the current transformation
   * @param row
   *   the row containing the new space values
   * @return
   *   an optional new transformation
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

trait ColumnStats extends Serializable {
  val statsNames: Seq[String]
  val statsSqlPredicates: Seq[String]
}

object ColumnStats {

  def apply(names: Seq[String], predicates: Seq[String]): ColumnStats =
    new ColumnStats {
      override val statsNames: Seq[String] = names
      override val statsSqlPredicates: Seq[String] = predicates
    }

}

/**
 * Empty ColumnStats
 */

object NoColumnStats extends ColumnStats {
  override val statsNames: Seq[String] = Nil
  override val statsSqlPredicates: Seq[String] = Nil
}

/**
 * Manual ColumnStats type
 * @param statsNames:
 *   the names of the stats
 */
case class ManualColumnStats(statsNames: Seq[String]) extends ColumnStats {
  override val statsSqlPredicates: Seq[String] = statsNames.map(name => s"null AS $name")
}
