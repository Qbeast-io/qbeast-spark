package io.qbeast.spark.internal.sources.catalog

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
