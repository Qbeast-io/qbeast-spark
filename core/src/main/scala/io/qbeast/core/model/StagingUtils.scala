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
package io.qbeast.core.model

import io.qbeast.core.transform.EmptyTransformer

trait StagingUtils {

  /**
   * RevisionID for the Staging Revision
   */
  protected val stagingID: RevisionID = 0L

  protected def isStaging(revisionID: RevisionID): Boolean = {
    revisionID == stagingID
  }

  protected def isStaging(revision: Revision): Boolean = {
    isStaging(revision.revisionID) &&
    revision.columnTransformers.forall {
      case _: EmptyTransformer => true
      case _ => false
    }
  }

  /**
   * Initialize Revision for table conversion. The RevisionID for a converted table is 0.
   * EmptyTransformers and EmptyTransformations are used. This Revision should always be
   * superseded.
   */
  protected def stagingRevision(
      tableID: QTableID,
      desiredCubeSize: Int,
      columnsToIndex: Seq[String]): Revision = {
    val emptyTransformers = columnsToIndex.map(s => EmptyTransformer(s)).toIndexedSeq
    val emptyTransformations = emptyTransformers.map(_.makeTransformation(r => r))

    Revision(
      stagingID,
      System.currentTimeMillis(),
      tableID,
      desiredCubeSize,
      emptyTransformers,
      emptyTransformations)
  }

}
