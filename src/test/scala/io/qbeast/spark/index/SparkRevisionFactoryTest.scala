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
package io.qbeast.spark.index

import io.qbeast.core.model._
import io.qbeast.core.model.QbeastOptions.CUBE_SIZE
import io.qbeast.core.transform.CDFNumericQuantilesTransformer
import io.qbeast.core.transform.CDFStringQuantilesTransformer
import io.qbeast.core.transform.HashTransformer
import io.qbeast.core.transform.LinearTransformer
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

class SparkRevisionFactoryTest extends QbeastIntegrationTestSpec {

  "SparkRevisionFactory" should "create a Revision with specified Transformer types" in withSpark {
    spark =>
      val schema = StructType(
        Seq(
          StructField("long_col", LongType),
          StructField("double_col", DoubleType),
          StructField("int_col", IntegerType),
          StructField("float_col", FloatType),
          StructField("str_col_1", StringType),
          StructField("str_col_2", StringType),
          StructField("str_col_3", StringType)))

      val columnsToIndex = Seq(
        "long_col:linear", // linear
        "double_col:hashing", // hashing
        "int_col:quantiles", // numeric quantiles
        "float_col", // defaults to linear
        "str_col_1:hashing", // hashing
        "str_col_2:quantiles", // string quantiles
        "str_col_3" // defaults to hashing
      )
      val options =
        QbeastOptions(Map("columnsToIndex" -> columnsToIndex.mkString(","), CUBE_SIZE -> "1000"))
      val tableId = QTableID("test")
      val revision = SparkRevisionFactory.createNewRevision(tableId, schema, options)

      revision.revisionID shouldBe 0
      revision.tableID shouldBe tableId
      revision.desiredCubeSize shouldBe 1000
      revision.columnTransformers shouldBe Vector(
        LinearTransformer("long_col", LongDataType),
        HashTransformer("double_col", DoubleDataType),
        CDFNumericQuantilesTransformer("int_col", IntegerDataType),
        LinearTransformer("float_col", FloatDataType),
        HashTransformer("str_col_1", StringDataType),
        CDFStringQuantilesTransformer("str_col_2"),
        HashTransformer("str_col_3", StringDataType))
      revision.transformations shouldBe Nil
  }

}
