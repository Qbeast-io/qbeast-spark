/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

package object model {
  type NormalizedWeight = Double
  type RevisionID = Long

  lazy val mapper: JsonMapper = {

    JsonMapper
      .builder()
      .addModule(DefaultScalaModule)
      .serializationInclusion(Include.NON_ABSENT)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .build()
  }

}
