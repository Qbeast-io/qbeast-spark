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
package io.qbeast.spark.delta

import io.qbeast.core.model.QbeastOptions
import io.qbeast.core.model.QbeastOptions.COLUMNS_TO_INDEX
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.qbeast.config.DEFAULT_CUBE_SIZE
import org.apache.spark.qbeast.config.DEFAULT_TABLE_FORMAT
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.DeltaOptions.MERGE_SCHEMA_OPTION
import org.apache.spark.sql.delta.DeltaOptions.OVERWRITE_SCHEMA_OPTION
import org.apache.spark.sql.delta.DeltaOptions.TXN_APP_ID
import org.apache.spark.sql.delta.DeltaOptions.TXN_VERSION
import org.apache.spark.sql.delta.DeltaOptions.USER_METADATA_OPTION

class QbeastOptionsDeltaTest extends QbeastIntegrationTestSpec {

  "QbeastOptions" should "be able to preserve settings when converted into a DeltaOptions" in withSpark {
    spark =>
      val options =
        QbeastOptions(
          Map(
            COLUMNS_TO_INDEX -> "id",
            TXN_APP_ID -> "app",
            TXN_VERSION -> "1",
            USER_METADATA_OPTION -> "Some metadata",
            MERGE_SCHEMA_OPTION -> "true",
            OVERWRITE_SCHEMA_OPTION -> "true"))
      val deltaOptions = new DeltaOptions(options.toMap, spark.sessionState.conf)

      deltaOptions.txnAppId shouldBe Some("app")
      deltaOptions.txnVersion shouldBe Some(1)
      deltaOptions.userMetadata shouldBe Some("Some metadata")
      deltaOptions.canMergeSchema shouldBe true
      deltaOptions.canOverwriteSchema shouldBe true

      options.columnsToIndex shouldBe Seq("id")
      options.cubeSize shouldBe DEFAULT_CUBE_SIZE
      options.tableFormat shouldBe DEFAULT_TABLE_FORMAT
      options.columnStats shouldBe None
  }

  it should "store additional delta-related options in extraOptions" in withSpark { _ =>
    val options =
      QbeastOptions(
        Map(
          COLUMNS_TO_INDEX -> "id",
          TXN_APP_ID -> "app",
          TXN_VERSION -> "1",
          USER_METADATA_OPTION -> "Some metadata",
          MERGE_SCHEMA_OPTION -> "true",
          OVERWRITE_SCHEMA_OPTION -> "true"))
    options.extraOptions(TXN_APP_ID) shouldBe "app"
    options.extraOptions(TXN_VERSION) shouldBe "1"
    options.extraOptions(USER_METADATA_OPTION) shouldBe "Some metadata"
    options.extraOptions(MERGE_SCHEMA_OPTION) shouldBe "true"
    options.extraOptions(OVERWRITE_SCHEMA_OPTION) shouldBe "true"

    options.columnsToIndex shouldBe Seq("id")
    options.cubeSize shouldBe DEFAULT_CUBE_SIZE
    options.tableFormat shouldBe DEFAULT_TABLE_FORMAT
    options.columnStats shouldBe None
  }

}
