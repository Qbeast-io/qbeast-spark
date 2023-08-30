/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql.delta

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{DeltaJobStatisticsTracker, StatisticsCollection}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.types.StructType

trait DeltaStatsCollectionUtils {

  protected def getDeltaOptionalTrackers(
      data: DataFrame,
      sparkSession: SparkSession,
      tableID: QTableID): Option[DeltaJobStatisticsTracker] = {

    if (QbeastContext.config.get(DeltaSQLConf.DELTA_COLLECT_STATS)) {
      val output = data.queryExecution.analyzed.output
      val statsDataSchema = output

      val deltaLog = DeltaLog.forTable(sparkSession, tableID.id)
      val metadata = deltaLog.unsafeVolatileMetadata
      val outputPath = deltaLog.dataPath
      val deltaProtocol = deltaLog.unsafeVolatileSnapshot.protocol

      val indexedCols = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(metadata)

      val statsCollection = new StatisticsCollection {

        override def dataSchema: StructType = statsDataSchema.toStructType

        override val spark: SparkSession = data.sparkSession

        override val numIndexedCols: Int = indexedCols

        override def tableDataSchema: StructType = data.schema

        override protected def protocol: Protocol = deltaProtocol
      }

      val statsColExpr: Expression = {
        val dummyDF = Dataset.ofRows(sparkSession, LocalRelation(statsDataSchema))
        dummyDF
          .select(to_json(statsCollection.statsCollector))
          .queryExecution
          .analyzed
          .expressions
          .head
      }

      Some(
        new DeltaJobStatisticsTracker(
          sparkSession.sessionState.newHadoopConf(),
          outputPath,
          statsDataSchema,
          statsColExpr))
    } else {
      None
    }
  }

}
