/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql

import org.apache.spark.sql.connector.catalog.CatalogPlugin

object SparkCatalogUtils {

  def getV2SessionCatalog(spark: SparkSession): CatalogPlugin = {
    val catalogManager = spark.sessionState.catalogManager
    catalogManager.v2SessionCatalog
  }

}
