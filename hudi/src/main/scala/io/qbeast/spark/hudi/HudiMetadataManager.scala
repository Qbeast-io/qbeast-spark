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
import io.qbeast.core.model.QbeastOptions
import io.qbeast.core.model.WriteMode.WriteModeValue
import io.qbeast.IISeq
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.model.HoodieTimelineTimeZone
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import java.nio.file.Paths
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

/**
 * Spark+Hudi implementation of the MetadataManager interface
 */
object HudiMetadataManager extends MetadataManager {

  private lazy val spark = SparkSession.active
  private lazy val hadoopConf = spark.sparkContext.hadoopConfiguration

  override def updateWithTransaction(
      tableID: QTableID,
      schema: StructType,
      options: QbeastOptions,
      writeMode: WriteModeValue)(
      writer: String => (TableChanges, IISeq[IndexFile], IISeq[DeleteFile])): Unit = {

    // Hudi requires the table name to be set. For now in here we obtain the table name from the path
    val extraOptions =
      if (!options.extraOptions.contains(HoodieWriteConfig.TBL_NAME.key))
        options.extraOptions ++ Map(
          HoodieWriteConfig.TBL_NAME.key -> Paths.get(tableID.id).getFileName.toString)
      else options.extraOptions
    val newOptions = options.copy(extraOptions = extraOptions)

    if (!existsLog(tableID)) createTable(tableID, newOptions.extraOptions)

    val metaClient = loadMetaClient(tableID)
    val metadataWriter =
      HudiMetadataWriter(tableID, writeMode, metaClient, newOptions, schema)

    metadataWriter.writeWithTransaction(writer)
  }

  override def updateMetadataWithTransaction(
      tableID: QTableID,
      dataSchema: StructType,
      overwrite: Boolean)(config: => Configuration): Unit = {

    val metaClient = loadMetaClient(tableID)
    val metadataWriter =
      HudiMetadataWriter(
        tableID,
        SaveMode.Append.toString,
        metaClient,
        QbeastOptions.empty,
        dataSchema)

    metadataWriter.updateMetadataWithTransaction(config, overwrite)

  }

  override def loadSnapshot(tableID: QTableID): HudiQbeastSnapshot = {
    HudiQbeastSnapshot(tableID)
  }

  override def loadCurrentSchema(tableID: QTableID): StructType = {
    loadSnapshot(tableID).schema
  }

  /**
   * Returns the MetaClient for the table
   * @param tableID
   *   the table ID
   * @return
   */
  private def loadMetaClient(tableID: QTableID): HoodieTableMetaClient = {
    HoodieTableMetaClient
      .builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(hadoopConf))
      .setBasePath(tableID.id)
      .build()
  }

  /**
   * Checks if there's an existing log directory for the table
   *
   * @param tableID
   *   the table ID
   * @return
   */
  override def existsLog(tableID: QTableID): Boolean = {
    val hoodiePath = new Path(tableID.id, HoodieTableMetaClient.METAFOLDER_NAME)
    val filesystem = FileSystem.get(new java.net.URI(tableID.id), hadoopConf)
    filesystem.exists(hoodiePath)
  }

  /**
   * Creates an initial log directory
   *
   * @param tableID
   *   Table ID
   */
  override def createLog(tableID: QTableID): Unit = {
    if (!existsLog(tableID)) {
      val hoodiePath = new Path(tableID.id, HoodieTableMetaClient.METAFOLDER_NAME)
      val filesystem = FileSystem.get(new java.net.URI(tableID.id), hadoopConf)
      filesystem.mkdirs(hoodiePath)
    }
  }

  /**
   * Creates the initial table files
   *
   * @param tableID
   *   Table ID
   */
  private def createTable(tableID: QTableID, tableConfigs: Map[String, String]): Unit = {
    val storageConfig = HadoopFSUtils.getStorageConfWithCopy(hadoopConf)
    val properties = TypedProperties.fromMap(tableConfigs.asJava)

    HoodieTableMetaClient
      .withPropertyBuilder()
      .setTableType(HoodieTableType.COPY_ON_WRITE)
      .setBaseFileFormat(HoodieFileFormat.PARQUET.name())
      .setCommitTimezone(HoodieTimelineTimeZone.LOCAL)
      .setHiveStylePartitioningEnable(false)
      .setPartitionMetafileUseBaseFormat(false)
      .setCDCEnabled(false)
      .setPopulateMetaFields(true)
      .setUrlEncodePartitioning(false)
      .setKeyGeneratorClassProp("org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .setDatabaseName("")
      .setPartitionFields("")
      .fromProperties(properties)
      .initTable(storageConfig, tableID.id)
  }

}
