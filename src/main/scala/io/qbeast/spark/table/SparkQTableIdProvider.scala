/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.model.{QTableID, QTableIDProvider}

class SparkQTableIdProvider extends QTableIDProvider {
  override def fromStringSerialization(value: String): QTableID = _
}
