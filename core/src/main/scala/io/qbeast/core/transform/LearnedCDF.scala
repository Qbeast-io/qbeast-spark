/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.transform

import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}

object DefaultLearnedCDF {
  val modelPath: String = "/default-xgboost-string-model.json"
  val treeLimit = 99
  val maxEncodingLength: Int = 30
  val paddingValue: Float = -1f

  def apply(): LearnedCDF = {
    val modelStream = getClass.getResourceAsStream(modelPath)
    val booster = XGBoost.loadModel(modelStream)
    new LearnedBoostingCDF(treeLimit, booster, maxEncodingLength, paddingValue)
  }

}

trait LearnedCDF {

  /**
   * The ML model used as a CDF for a given String column.
   */
  val model: Any

  /**
   * The size of the encoding vector for input Strings.
   */
  val maxEncodingLength: Int

  /**
   * Pad value for Strings shorter than maxEncodingLength.
   */
  val paddingValue: Float

  /**
   * Predict input string percentile rank, the output is clipped within the range of
   * minValue and maxValue, both included.
   */
  def predict(str: String, minValue: Float, maxValue: Float): Double

  /**
   * Encode input string into a sequence of floats.
   */
  def encodeString(str: String): Array[Float]
}

/**
 * Trained XGBOOST model as a Cumulative Distribution Function for string column indexing
 * @param treeLimit best iteration
 * @param model XGBRegressor
 * @param maxEncodingLength input string encoding length
 * @param paddingValue pad value for shorter strings
 */
class LearnedBoostingCDF(
    treeLimit: Int,
    val model: Booster,
    val maxEncodingLength: Int,
    val paddingValue: Float)
    extends Serializable
    with LearnedCDF {

  def predict(str: String, minValue: Float, maxValue: Float): Double = {
    val dMatrix =
      new DMatrix(encodeString(str), 1, maxEncodingLength, paddingValue)
    val pred = model.predict(data = dMatrix, treeLimit = treeLimit).head.head

    pred.max(minValue).min(maxValue)
  }

  def encodeString(str: String): Array[Float] = {
    val encodingBuilder = Array.newBuilder[Float]
    encodingBuilder.sizeHint(maxEncodingLength)

    0 until maxEncodingLength foreach { i =>
      val encoding =
        if (i < str.length) str(i).toFloat
        else -1f
      encodingBuilder += encoding
    }

    val encoding = encodingBuilder.result()
    encoding
  }

}
