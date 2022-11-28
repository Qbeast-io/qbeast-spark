/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

package object model {
  type NormalizedWeight = Double
  type RevisionID = Long
  type LocalTree = Map[CubeId, CubeInfo]

  /**
   * ReplicatedSet is used to represent a set of CubeId's that had been replicated
   */
  type ReplicatedSet = Set[CubeId]

  lazy val mapper: JsonMapper = {

    JsonMapper
      .builder()
      .addModule(DefaultScalaModule)
      .serializationInclusion(Include.NON_ABSENT)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .build()
  }

}
