/*
 * Copyright 2024 Qbeast Analytics, S.L.
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
package io.qbeast.core.stats.tracker

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, DeclarativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateMutableProjection
import org.apache.spark.sql.catalyst.expressions.{Attribute, EmptyRow, Expression, GenericInternalRow, MutableProjection, Projection, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.{WriteJobStatsTracker, WriteTaskStats, WriteTaskStatsTracker}
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable

case class TaskStats(writeTaskStats: Seq[WriteTaskStats], endTime: Long)

object StatsTracker {

  private var statsTrackers: Seq[WriteJobStatsTracker] = Seq.empty

  def registerStatsTrackers(newStatsTrackers: Seq[WriteJobStatsTracker]): Unit = {
    statsTrackers = newStatsTrackers
  }

  def getStatsTrackers(): Seq[WriteJobStatsTracker] = statsTrackers

}

case class FileStatistics(stats: Map[String, String]) extends WriteTaskStats

class TaskStatisticsTracker(
                             dataCols: Seq[Attribute],
                             statsColExpr: Expression,
                             rootPath: Path,
                             hadoopConf: Configuration) extends WriteTaskStatsTracker {

  protected[this] val submittedFiles = mutable.HashMap[String, InternalRow]()

  // For example, when strings are involved, statsColExpr might look like
  // struct(
  //   count(new Column("*")) as "numRecords"
  //   struct(
  //     substring(min(col), 0, stringPrefix))
  //   ) as "minValues",
  //   struct(
  //     udf(max(col))
  //   ) as "maxValues"
  // ) as "stats"

  // [[DeclarativeAggregate]] is the API to the Catalyst machinery for initializing and updating
  // the result of an aggregate function. We will be using it here the same way it's used during
  // query execution.

  // Given the example above, aggregates would hold: Seq(count, min, max)
  private val aggregates: Seq[DeclarativeAggregate] = statsColExpr.collect {
    case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
      ae.aggregateFunction.asInstanceOf[DeclarativeAggregate]
  }

  // The fields of aggBuffer - see below
  protected val aggBufferAttrs: Seq[Attribute] = aggregates.flatMap(_.aggBufferAttributes)

  // This projection initializes aggBuffer with the neutral values for the agg fcns e.g. 0 for sum
  protected val initializeStats: MutableProjection = GenerateMutableProjection.generate(
    expressions = aggregates.flatMap(_.initialValues),
    inputSchema = Seq.empty,
    useSubexprElimination = false
  )

  // This projection combines the intermediate results stored by aggBuffer with the values of the
  // currently processed row and updates aggBuffer in place.
  private val updateStats: MutableProjection = GenerateMutableProjection.generate(
    expressions = JoinedProjection.bind(
      aggBufferAttrs,
      dataCols,
      aggregates.flatMap(_.updateExpressions)),
    inputSchema = Nil,
    useSubexprElimination = true
  )

  // This executes the whole statsColExpr in order to compute the final stats value for the file.
  // In order to evaluate it, we have to replace its aggregate functions with the corresponding
  // aggregates' evaluateExpressions that basically just return the results stored in aggBuffer.
  private val resultExpr: Expression = statsColExpr.transform {
    case ae: AggregateExpression if ae.aggregateFunction.isInstanceOf[DeclarativeAggregate] =>
      ae.aggregateFunction.asInstanceOf[DeclarativeAggregate].evaluateExpression
  }

  // See resultExpr above
  private val getStats: Projection = UnsafeProjection.create(
    exprs = Seq(resultExpr),
    inputSchema = aggBufferAttrs
  )

  // This serves as input to updateStats, with aggBuffer always on the left, while the right side
  // is every time replaced with the row currently being processed - see updateStats and newRow.
  private val extendedRow: GenericInternalRow = new GenericInternalRow(2)

  // file path to corresponding stats encoded as json
  protected val results = new collection.mutable.HashMap[String, String]

  // called once per file, executes the getStats projection
  override def closeFile(filePath: String): Unit = {
    // We assume file names are unique
    val fileName = new Path(filePath).getName

    assert(!results.contains(fileName), s"Stats already recorded for file: $filePath")
    // this is statsColExpr's output (json string)
    val jsonStats = getStats(submittedFiles(filePath)).getString(0)
    results += ((fileName, jsonStats))
    submittedFiles.remove(filePath)
  }

  override def newPartition(partitionValues: InternalRow): Unit = { }

  protected def initializeAggBuf(buffer: SpecificInternalRow): InternalRow =
    initializeStats.target(buffer).apply(EmptyRow)

  override def newFile(newFilePath: String): Unit = {
    submittedFiles.getOrElseUpdate(newFilePath, {
      // `buffer` is a row that will start off by holding the initial values for the agg expressions
      // (see the initializeStats: Projection), will then be updated in place every time a new row
      // is processed (see updateStats: Projection), and will finally serve as an input for
      // computing the per-file result of statsColExpr (see getStats: Projection)
      val buffer = new SpecificInternalRow(aggBufferAttrs.map(_.dataType))
      initializeAggBuf(buffer)
    })
  }

  override def newRow(filePath: String, currentRow: InternalRow): Unit = {
    val aggBuffer = submittedFiles(filePath)
    extendedRow.update(0, aggBuffer)
    extendedRow.update(1, currentRow)
    updateStats.target(aggBuffer).apply(extendedRow)
  }

  override def getFinalStats(taskCommitTime: Long): FileStatistics = {
    submittedFiles.keys.foreach(closeFile)
    submittedFiles.clear()
    FileStatistics(results.toMap)
  }
}

class JobStatisticsTracker(
                            @transient private val hadoopConf: Configuration,
                            @transient val path: Path,
                            val dataCols: Seq[Attribute],
                            val statsColExpr: Expression
                          ) extends WriteJobStatsTracker {

  var recordedStats: Map[String, String] = _

  private val srlHadoopConf = new SerializableConfiguration(hadoopConf)
  private val rootUri = path.getFileSystem(hadoopConf).makeQualified(path).toUri()

  override def newTaskInstance(): WriteTaskStatsTracker = {
    val rootPath = new Path(rootUri)
    val hadoopConf = srlHadoopConf.value
    new TaskStatisticsTracker(dataCols, statsColExpr, rootPath, hadoopConf)
  }

  override def processStats(stats: Seq[WriteTaskStats], jobCommitTime: Long): Unit = {
    recordedStats = stats.map(_.asInstanceOf[FileStatistics]).flatMap(_.stats).toMap
  }
}