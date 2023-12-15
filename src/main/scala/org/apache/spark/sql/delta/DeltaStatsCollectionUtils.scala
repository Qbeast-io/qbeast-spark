/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package org.apache.spark.sql.delta

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QTableID
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaJobStatisticsTracker
import org.apache.spark.sql.delta.stats.DeltaStatsColumnSpec
import org.apache.spark.sql.delta.stats.StatisticsCollection
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

trait DeltaStatsCollectionUtils {

  /**
   * Return a tuple of (outputStatsCollectionSchema, statsCollectionSchema).
   * outputStatsCollectionSchema is the data source schema from DataFrame used for stats
   * collection. It contains the columns in the DataFrame output, excluding the partition columns.
   * tableStatsCollectionSchema is the schema to collect stats for. It contains the columns in the
   * table schema, excluding the partition columns. Note: We only collect NULL_COUNT stats (as the
   * number of rows) for the columns in statsCollectionSchema but missing in
   * outputStatsCollectionSchema
   */
  protected def getStatsSchema(
      metadata: Metadata,
      dataFrameOutput: Seq[Attribute],
      partitionSchema: StructType): (Seq[Attribute], Seq[Attribute]) = {
    val partitionColNames = partitionSchema.map(_.name).toSet

    // The outputStatsCollectionSchema comes from DataFrame output
    // schema should be normalized, therefore we can do an equality check
    val outputStatsCollectionSchema = dataFrameOutput
      .filterNot(c => partitionColNames.contains(c.name))

    // The tableStatsCollectionSchema comes from table schema
    val statsTableSchema = toAttributes(metadata.schema)
    val mappedStatsTableSchema = if (metadata.columnMappingMode == NoMapping) {
      statsTableSchema
    } else {
      DeltaColumnMapping.createPhysicalAttributes(
        statsTableSchema,
        metadata.schema,
        metadata.columnMappingMode)
    }

    // It's important to first do the column mapping and then drop the partition columns
    val tableStatsCollectionSchema = mappedStatsTableSchema
      .filterNot(c => partitionColNames.contains(c.name))

    (outputStatsCollectionSchema, tableStatsCollectionSchema)
  }

  protected def getDeltaOptionalTrackers(
      data: DataFrame,
      sparkSession: SparkSession,
      tableID: QTableID): Option[DeltaJobStatisticsTracker] = {

    if (QbeastContext.config.get(DeltaSQLConf.DELTA_COLLECT_STATS)) {
      val outputStatsAtrributes = data.queryExecution.analyzed.output

      val deltaLog = DeltaLog.forTable(sparkSession, tableID.id)
      val deltaSnapshot = deltaLog.update()
      val deltaMetadata = deltaSnapshot.metadata
      val outputPath = deltaLog.dataPath

      val (outputStatsCollectionSchema, tableStatsCollectionSchema) = getStatsSchema(
        deltaMetadata,
        outputStatsAtrributes,
        deltaSnapshot.metadata.partitionSchema)

      val statsCollection = new StatisticsCollection {

        override val spark: SparkSession = data.sparkSession

        override protected def protocol: Protocol = deltaSnapshot.protocol

        override def tableSchema: StructType = deltaMetadata.schema

        override def outputTableStatsSchema: StructType = {
          // If collecting stats uses the table schema, then we pass in tableStatsCollectionSchema;
          // otherwise, pass in outputStatsCollectionSchema to collect stats using the DataFrame
          // schema.
          if (spark.sessionState.conf.getConf(
              DeltaSQLConf.DELTA_COLLECT_STATS_USING_TABLE_SCHEMA)) {
            tableStatsCollectionSchema.toStructType
          } else {
            outputStatsCollectionSchema.toStructType
          }
        }

        override def outputAttributeSchema: StructType = outputStatsCollectionSchema.toStructType

        override val statsColumnSpec: DeltaStatsColumnSpec =
          StatisticsCollection.configuredDeltaStatsColumnSpec(deltaMetadata)

        override def columnMappingMode: DeltaColumnMappingMode = deltaSnapshot.columnMappingMode
      }

      val statsColExpr: Expression =
        Dataset
          .ofRows(sparkSession, LocalRelation(outputStatsCollectionSchema))
          .select(to_json(statsCollection.statsCollector))
          .queryExecution
          .analyzed
          .expressions
          .head

      Some(
        new DeltaJobStatisticsTracker(
          deltaLog.newDeltaHadoopConf(),
          outputPath,
          outputStatsCollectionSchema,
          statsColExpr))
    } else {
      None
    }
  }

}
