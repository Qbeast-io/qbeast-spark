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
package io.qbeast.spark.sources.catalog

import org.apache.spark.sql.SaveMode

trait CreationMode {
  val saveMode: SaveMode
  val name: String
}

object TableCreationMode {

  val CREATE_TABLE: CreationMode = new CreationMode {
    override val saveMode: SaveMode = SaveMode.ErrorIfExists
    override val name: String = "create"
  }

  val CREATE_OR_REPLACE: CreationMode = new CreationMode {
    override val saveMode: SaveMode = SaveMode.Overwrite
    override val name: String = "createOrReplace"
  }

  val REPLACE_TABLE: CreationMode = new CreationMode {
    override val saveMode: SaveMode = SaveMode.Overwrite
    override val name: String = "replace"
  }

}
