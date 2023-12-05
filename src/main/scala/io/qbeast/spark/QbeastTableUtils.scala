package io.qbeast.spark

import io.qbeast.spark.delta.DeltaQbeastSnapshot
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaTableUtils.isDeltaTable

object QbeastTableUtils {

  /**
   * Gets the existing configuration of a Qbeast table
   * @param spark
   * @param path
   * @return
   */
  def getExistingConf(spark: SparkSession, path: Path): Map[String, String] = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val unsafeVolatileSnapshot = deltaLog.update()
    val qbeastSnapshot = DeltaQbeastSnapshot(unsafeVolatileSnapshot)
    val revision = qbeastSnapshot.loadLatestRevision
    Map(
      "columnsToIndex" -> revision.columnTransformers.map(_.columnName).mkString(","),
      "cubeSize" -> revision.desiredCubeSize.toString)

  }

  /**
   * Checks if a table is a Qbeast table
   * @param path
   * @return
   */

  def isQbeastTable(path: Path): Boolean = {
    val spark = SparkSession.active

    isDeltaTable(spark, path) && {
      val deltaLog = DeltaLog.forTable(spark, path)
      val unsafeVolatileSnapshot = deltaLog.update()
      val qbeastSnapshot = DeltaQbeastSnapshot(unsafeVolatileSnapshot)
      val isQbeast = qbeastSnapshot.loadAllRevisions.nonEmpty
      isQbeast
    }
  }

}
