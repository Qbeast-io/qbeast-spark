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
package io.qbeast.spark.hudi

import io.qbeast.core.model._
import io.qbeast.core.model.PreCommitHook.PreCommitHookOutput
import io.qbeast.core.model.WriteMode.WriteModeValue
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.spark.utils.TagUtils
import io.qbeast.spark.writer.StatsTracker.registerStatsTrackers
import io.qbeast.IISeq
import org.apache.avro.Schema
import org.apache.hudi
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.ActionType
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.model.HoodiePartitionMetadata
import org.apache.hudi.common.model.HoodiePayloadProps
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE
import org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.util.CommitUtils
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.common.util.StringUtils.getUTF8Bytes
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.SerDeHelper
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.AvroConversionUtils
import org.apache.hudi.AvroConversionUtils.getAvroRecordNameAndNamespace
import org.apache.hudi.DataSourceOptionsHelper.fetchMissingWriteConfigsFromTableConfig
import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceUtils
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieSchemaUtils
import org.apache.hudi.HoodieWriterUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.datasources.WriteJobStatsTracker
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

import java.lang.System.currentTimeMillis
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
 * DeltaMetadataWriter is in charge of writing data to a table and report the necessary log
 * information
 *
 * @param tableID
 *   the table identifier
 * @param writeMode
 *   writeMode of the metadata
 * @param metaClient
 *   metaClient associated to the table
 * @param qbeastOptions
 *   options for writeMetadata operation
 * @param schema
 *   the schema of the table
 */
private[hudi] case class HudiMetadataWriter(
    tableID: QTableID,
    writeMode: WriteModeValue,
    metaClient: HoodieTableMetaClient,
    qbeastOptions: QbeastOptions,
    schema: StructType)
    extends HudiMetadataOperation
    with Logging {

  private val spark = SparkSession.active

  private val basePath = tableID.id

  private val jsc = new JavaSparkContext(spark.sparkContext)

  private def isOverwriteOperation: Boolean = writeMode == WriteMode.Overwrite

  private def isOptimizeOperation: Boolean = writeMode == WriteMode.Optimize

  // As in Delta, currently we treat the schema of data written to Delta is
  // nullable=true because  it can come from any places and these random places
  // may not respect nullable very well.
  private val dataSchema: StructType = StructType(schema.fields.map(field =>
    field.copy(nullable = true, dataType = asNullable(field.dataType))))

  /**
   * Creates an instance of basic stats tracker on the desired transaction
   * @return
   */
  private def createStatsTrackers(): Seq[WriteJobStatsTracker] = {
    val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()
    // Create basic stats trackers to add metrics on the Write Operation
    val hadoopConf = spark.sessionState.newHadoopConf()
    val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
      new SerializableConfiguration(hadoopConf),
      BasicWriteJobStatsTracker.metrics)
    statsTrackers.append(basicWriteJobStatsTracker)
    statsTrackers
  }

  private val preCommitHooks = new ListBuffer[PreCommitHook]()

  // Load the pre-commit hooks
  loadPreCommitHooks().foreach(registerPreCommitHooks)

  /**
   * Register a pre-commit hook
   * @param preCommitHook
   *   the hook to register
   */
  private def registerPreCommitHooks(preCommitHook: PreCommitHook): Unit = {
    if (!preCommitHooks.contains(preCommitHook)) {
      preCommitHooks.append(preCommitHook)
    }
  }

  /**
   * Load the pre-commit hooks from the options
   * @return
   *   the loaded hooks
   */
  private def loadPreCommitHooks(): Seq[PreCommitHook] =
    qbeastOptions.hookInfo.map(QbeastHookLoader.loadHook)

  /**
   * Executes all registered pre-commit hooks.
   *
   * This function iterates over all pre-commit hooks registered in the `preCommitHooks`
   * ArrayBuffer. For each hook, it calls the `run` method of the hook, passing the provided
   * actions as an argument. The `run` method of a hook is expected to return a Map[String,
   * String] which represents the output of the hook. The outputs of all hooks are combined into a
   * single Map[String, String] which is returned as the result of this function.
   *
   * It's important to note that if two or more hooks return a map with the same key, the value of
   * the key in the resulting map will be the value from the last hook that returned that key.
   * This is because the `++` operation on maps in Scala is a right-biased union operation, which
   * means that if there are duplicate keys, the value from the right operand (in this case, the
   * later hook) will overwrite the value from the left operand.
   *
   * Therefore, to avoid unexpected behavior, it's crucial to ensure that the outputs of different
   * hooks have unique keys. If there's a possibility of key overlap, the hooks should be designed
   * to handle this appropriately, for example by prefixing each key with a unique identifier for
   * the hook.
   *
   * @param actions
   *   The actions to be passed to the `run` method of each hook.
   * @return
   *   A Map[String, String] representing the combined outputs of all hooks.
   */
  private def runPreCommitHooks(actions: Seq[QbeastFile]): PreCommitHookOutput = {
    preCommitHooks.foldLeft(Map.empty[String, String]) { (acc, hook) =>
      acc ++ hook.run(actions)
    }
  }

  private def createHoodieClient(): SparkRDDWriteClient[_] = {

    val (parameters, hoodieConfig) = mergeParamsAndGetHoodieConfig(
      qbeastOptions.extraOptions,
      metaClient.getTableConfig,
      if (isOverwriteOperation) SaveMode.Overwrite else SaveMode.Append)

    val tblName = hoodieConfig
      .getStringOrThrow(
        HoodieWriteConfig.TBL_NAME,
        s"'${HoodieWriteConfig.TBL_NAME.key}' must be set.")
      .trim

    val shouldReconcileSchema = parameters(
      DataSourceWriteOptions.RECONCILE_SCHEMA.key()).toBoolean

    val latestTableSchemaOpt = toScalaOption(
      new TableSchemaResolver(metaClient).getTableAvroSchemaFromLatestCommit(false))

    val (avroRecordName, avroRecordNamespace) = latestTableSchemaOpt
      .map(s => (s.getName, s.getNamespace))
      .getOrElse(getAvroRecordNameAndNamespace(tblName))

    val sourceSchema = AvroConversionUtils
      .convertStructTypeToAvroSchema(dataSchema, avroRecordName, avroRecordNamespace)

    val internalSchemaOpt =
      HoodieSchemaUtils.getLatestTableInternalSchema(hoodieConfig, metaClient).orElse {
        // In case we need to reconcile the schema and schema evolution is enabled,
        // we will force-apply schema evolution to the writer's schema
        if (shouldReconcileSchema && hoodieConfig.getBooleanOrDefault(
            DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED)) {
          val allowOperationMetaDataField = parameters
            .getOrElse(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key(), "false")
            .toBoolean
          Some(
            AvroInternalSchemaConverter.convert(
              HoodieAvroUtils.addMetadataFields(
                latestTableSchemaOpt.getOrElse(sourceSchema),
                allowOperationMetaDataField)))
        } else {
          None
        }
      }

    val writerSchema = HoodieSchemaUtils.deduceWriterSchema(
      sourceSchema,
      latestTableSchemaOpt,
      internalSchemaOpt,
      parameters)

    val finalOpts = addSchemaEvolutionParameters(
      parameters,
      internalSchemaOpt,
      Some(writerSchema)) - HoodieWriteConfig.AUTO_COMMIT_ENABLE.key

    // Check if it is necessary in hudi
    val statsTrackers = createStatsTrackers()
    registerStatsTrackers(statsTrackers)

    DataSourceUtils.createHoodieClient(
      jsc,
      writerSchema.toString,
      basePath,
      tblName,
      finalOpts.asJava)

  }

  def writeWithTransaction(
      writer: String => (TableChanges, Seq[IndexFile], Seq[DeleteFile])): Unit = {

    val isNewTable = metaClient.getCommitsTimeline.filterCompletedInstants.countInstants() == 0

    val hudiClient = createHoodieClient()

    val operationType =
      if (isOptimizeOperation || (isOverwriteOperation && !isNewTable))
        WriteOperationType.INSERT_OVERWRITE
      else WriteOperationType.BULK_INSERT

    val commitActionType = CommitUtils.getCommitActionType(operationType, metaClient.getTableType)

    val instantTime = HoodieActiveTimeline.createNewInstantTime
    hudiClient.startCommitWithTime(instantTime, commitActionType)
    hudiClient.setOperationType(operationType)

    val hoodieTable = HoodieSparkTable.create(hudiClient.getConfig, hudiClient.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(operationType)
    timeLine.transitionRequestedToInflight(
      requested,
      hudi.common.util.Option.of(getUTF8Bytes(metadata.toJsonString)))

    val extraMeta = new util.HashMap[String, String]()

    val currTimer = HoodieTimer.start()
    val (tableChanges, indexFiles, deleteFiles) = writer(instantTime)
    val totalWriteTime = currTimer.endTimer()

    val qbeastFiles =
      updateMetadata(
        isNewTable,
        instantTime,
        tableChanges,
        indexFiles,
        deleteFiles,
        qbeastOptions.extraOptions)

    val qbeastMetadata: mutable.Map[String, Map[String, Object]] = mutable.Map()
    val writeStatusList = ListBuffer[WriteStatus]()
    indexFiles.foreach { indexFile =>
      val writeStatus = HudiQbeastFileUtils.toWriteStat(indexFile, totalWriteTime)
      writeStatusList += writeStatus
      val metadata = Map(
        TagUtils.revision -> indexFile.revisionId.toString,
        TagUtils.blocks -> mapper.readTree(HudiQbeastFileUtils.encodeBlocks(indexFile.blocks)))
      qbeastMetadata += (indexFile.path -> metadata)
    }

    val tags = runPreCommitHooks(qbeastFiles)

    extraMeta.put(MetadataConfig.tags, mapper.writeValueAsString(tags))
    extraMeta.put(MetadataConfig.blocks, mapper.writeValueAsString(qbeastMetadata))

    val writeStatusRdd = jsc.parallelize(writeStatusList)

    if (commitActionType == ActionType.replacecommit.toString) {
      val replacedFileIds = if (isOptimizeOperation) {
        getReplacedFileIdsForOptimize(deleteFiles)
      } else {
        getReplacedFileIdsForPartition(hoodieTable, basePath)
      }
      hudiClient.commit(
        instantTime,
        writeStatusRdd,
        hudi.common.util.Option.of(extraMeta),
        commitActionType,
        replacedFileIds)
    } else {
      hudiClient.commit(instantTime, writeStatusRdd, hudi.common.util.Option.of(extraMeta))
    }

  }

  def updateMetadataWithTransaction(config: => Configuration, overwrite: Boolean): Unit = {
    val updatedConfig =
      if (overwrite) config
      else
        config.foldLeft(getCurrentConfig) { case (accConf, (k, v)) =>
          accConf.updated(k, v)
        }

    val hudiClient = createHoodieClient()
    val commitActionType =
      CommitUtils.getCommitActionType(WriteOperationType.ALTER_SCHEMA, metaClient.getTableType)
    val instantTime = HoodieActiveTimeline.createNewInstantTime

    hudiClient.startCommitWithTime(instantTime, commitActionType)
    hudiClient.preWrite(instantTime, WriteOperationType.ALTER_SCHEMA, metaClient)

    val hoodieTable = HoodieSparkTable.create(hudiClient.getConfig, hudiClient.getEngineContext)
    val timeLine = hoodieTable.getActiveTimeline
    val requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime)
    val metadata = new HoodieCommitMetadata
    metadata.setOperationType(WriteOperationType.ALTER_SCHEMA)
    timeLine.transitionRequestedToInflight(
      requested,
      hudi.common.util.Option.of(getUTF8Bytes(metadata.toJsonString)))

    updateTableMetadata(
      metaClient,
      isOverwriteOperation,
      updatedConfig,
      hasRevisionUpdate = true // Force update the metadata
    )

    hudiClient.commit(instantTime, jsc.emptyRDD)
  }

  /**
   * Writes metadata of the table
   *
   * @param tableChanges
   *   changes to apply
   * @param indexFiles
   *   files to add
   * @param deleteFiles
   *   files to remove
   * @param extraConfiguration
   *   extra configuration to apply
   * @return
   *   the sequence of file actions to save in the commit log(add, remove...)
   */
  protected def updateMetadata(
      isNewTable: Boolean,
      instantTime: String,
      tableChanges: TableChanges,
      indexFiles: Seq[IndexFile],
      deleteFiles: Seq[DeleteFile],
      extraConfiguration: Configuration): Seq[QbeastFile] = {

    val hasPartitionMetadata = HoodiePartitionMetadata.hasPartitionMetadata(
      metaClient.getStorage,
      metaClient.getBasePathV2)
    if (!hasPartitionMetadata) {
      val partitionMetadata =
        new HoodiePartitionMetadata(
          metaClient.getStorage,
          instantTime,
          metaClient.getBasePathV2,
          metaClient.getBasePathV2,
          hudi.common.util.Option.empty())
      partitionMetadata.trySave()
    }

    // Update the qbeast configuration with new metadata if needed
    val (newConfiguration, hasRevisionUpdate) = updateConfiguration(
      getCurrentConfig,
      isNewTable,
      isOverwriteOperation,
      tableChanges,
      qbeastOptions)

    // Write the qbeast configuration to the table properties metadata
    updateTableMetadata(metaClient, isOverwriteOperation, newConfiguration, hasRevisionUpdate)

    val deletedFiles = if (isOverwriteOperation && !isNewTable) {
      getAllIndexFiles.map { indexFile =>
        DeleteFile(
          path = indexFile.path,
          size = indexFile.size,
          dataChange = true,
          deletionTimestamp = currentTimeMillis())
      }.toIndexedSeq
    } else deleteFiles

    indexFiles ++ deletedFiles
  }

  private def getCurrentConfig: Configuration = {
    val tablePropsMap = metaClient.getTableConfig.getProps.asScala.toMap
    tablePropsMap
      .get(MetadataConfig.configuration)
      .map { configJson =>
        mapper.readValue[Map[String, String]](configJson, classOf[Map[String, String]])
      }
      .getOrElse(Map.empty)
  }

  private def getAllIndexFiles: IISeq[IndexFile] = {
    val tableMetadata: HoodieTableMetadata =
      new HoodieBackedTableMetadata(
        new HoodieSparkEngineContext(jsc),
        metaClient.getStorage,
        HoodieMetadataConfig.newBuilder().enable(true).build(),
        tableID.id,
        false)
    val tablePath = new StoragePath(tableID.id)
    val allFilesM = tableMetadata.getAllFilesInPartition(tablePath).asScala
    allFilesM
      .map(fileStatus =>
        new IndexFileBuilder()
          .setPath(fileStatus.getPath.getName)
          .setSize(fileStatus.getLength)
          .setDataChange(false)
          .setModificationTime(fileStatus.getModificationTime)
          .result())
      .toIndexedSeq
  }

  private def getReplacedFileIdsForPartition(
      hoodieTable: HoodieSparkTable[_],
      partitionPath: String): java.util.Map[String, java.util.List[String]] = {
    val existingFileIds = hoodieTable.getSliceView
      .getLatestFileSlices(partitionPath)
      .distinct
      .collect(java.util.stream.Collectors.toList())
      .asScala
      .map(_.getFileId)
      .toList
      .asJava

    Map("" -> existingFileIds).asJava
  }

  private def getReplacedFileIdsForOptimize(
      deleteFiles: Seq[DeleteFile]): java.util.Map[String, java.util.List[String]] = {

    val fileUUIDs = deleteFiles.map(deleteFile => FSUtils.getFileId(deleteFile.path))

    Map("" -> fileUUIDs.asJava).asJava
  }

  private def mergeParamsAndGetHoodieConfig(
      optParams: Map[String, String],
      tableConfig: HoodieTableConfig,
      mode: SaveMode): (Map[String, String], HoodieConfig) = {
    val translatedOptions = DataSourceWriteOptions.mayBeDerivePartitionPath(optParams)
    val translatedOptsWithMappedTableConfig = mutable.Map.empty ++ translatedOptions
    if (tableConfig != null && mode != SaveMode.Overwrite) {
      // for missing write configs corresponding to table configs, fill them up.
      fetchMissingWriteConfigsFromTableConfig(tableConfig, optParams).foreach(kv =>
        translatedOptsWithMappedTableConfig += (kv._1 -> kv._2))
    }
    if (null != tableConfig && mode != SaveMode.Overwrite) {
      // over-ride only if not explicitly set by the user.
      tableConfig.getProps.asScala
        .filter(kv => !optParams.contains(kv._1))
        .foreach { case (key, value) =>
          translatedOptsWithMappedTableConfig += (key -> value)
        }
    }
    val mergedParams = mutable.Map.empty ++ HoodieWriterUtils.parametersWithWriteDefaults(
      translatedOptsWithMappedTableConfig.toMap)
    if (!mergedParams.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key)
      && mergedParams.contains(KEYGENERATOR_CLASS_NAME.key)) {
      mergedParams(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key) = mergedParams(
        DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key)
    }
    // use preCombineField to fill in PAYLOAD_ORDERING_FIELD_PROP_KEY
    if (mergedParams.contains(PRECOMBINE_FIELD.key())) {
      mergedParams.put(
        HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY,
        mergedParams(PRECOMBINE_FIELD.key()))
    }
    if (mergedParams(OPERATION.key()) == INSERT_OPERATION_OPT_VAL && mergedParams
        .contains(DataSourceWriteOptions.INSERT_DUP_POLICY.key())
      && mergedParams(DataSourceWriteOptions.INSERT_DUP_POLICY.key()) != FAIL_INSERT_DUP_POLICY) {
      // enable merge allow duplicates when operation type is insert
      mergedParams.put(HoodieWriteConfig.MERGE_ALLOW_DUPLICATE_ON_INSERTS_ENABLE.key(), "true")
    }
    // disable drop partition columns when upsert MOR table
    if (mergedParams(OPERATION.key) == UPSERT_OPERATION_OPT_VAL
      && mergedParams.getOrElse(
        DataSourceWriteOptions.TABLE_TYPE.key,
        COPY_ON_WRITE.name) == MERGE_ON_READ.name) {
      mergedParams.put(HoodieTableConfig.DROP_PARTITION_COLUMNS.key, "false")
    }

    val params = mergedParams.toMap
    (params, HoodieWriterUtils.convertMapToHoodieConfig(params))
  }

  private def addSchemaEvolutionParameters(
      parameters: Map[String, String],
      internalSchemaOpt: scala.Option[InternalSchema],
      writeSchemaOpt: scala.Option[Schema]): Map[String, String] = {
    val schemaEvolutionEnable = if (internalSchemaOpt.isDefined) "true" else "false"

    val schemaValidateEnable =
      if (schemaEvolutionEnable.toBoolean && parameters
          .getOrElse(DataSourceWriteOptions.RECONCILE_SCHEMA.key(), "false")
          .toBoolean) {
        // force disable schema validate, now we support schema evolution, no need to do validate
        "false"
      } else {
        parameters.getOrElse(HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key(), "true")
      }
    // correct internalSchema, internalSchema should contain hoodie metadata columns.
    val correctInternalSchema = internalSchemaOpt.map { internalSchema =>
      if (internalSchema.findField(
          HoodieRecord.RECORD_KEY_METADATA_FIELD) == null && writeSchemaOpt.isDefined) {
        val allowOperationMetaDataField = parameters
          .getOrElse(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key(), "false")
          .toBoolean
        AvroInternalSchemaConverter.convert(
          HoodieAvroUtils.addMetadataFields(writeSchemaOpt.get, allowOperationMetaDataField))
      } else {
        internalSchema
      }
    }
    parameters ++ Map(
      HoodieWriteConfig.INTERNAL_SCHEMA_STRING.key() -> SerDeHelper.toJson(
        correctInternalSchema.orNull),
      HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key() -> schemaEvolutionEnable,
      HoodieWriteConfig.AVRO_SCHEMA_VALIDATE_ENABLE.key() -> schemaValidateEnable)
  }

}
