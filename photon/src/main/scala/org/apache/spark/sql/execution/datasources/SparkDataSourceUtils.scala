package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{AnalysisExceptionFactory, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * Contains utility methods to interact with the datasource
 */
object SparkDataSourceUtils {

  def getPartitionFiltersAndDataFilters(
      partitionSchema: StructType,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    DataSourceUtils.getPartitionFiltersAndDataFilters(partitionSchema, filters)

  }

  def translateFilter(
      filterExpr: Expression,
      supportNestedPredicatePushdown: Boolean): Option[Filter] = {
    DataSourceStrategy.translateFilter(filterExpr, supportNestedPredicatePushdown)
  }

  def mapFiltersToV2(filters: Seq[Filter]): Seq[Predicate] = filters.map(_.toV2)

  private def qualifiedPathName(path: String, hadoopConf: Configuration): String = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toString
  }

  def getTableName(
      options: Map[String, String],
      paths: Seq[String],
      shortName: String): String = {
    val sparkSession = SparkSession.active
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(options)
    shortName + " " + paths.map(qualifiedPathName(_, hadoopConf)).mkString(",")
  }

  def getPath(options: Map[String, String]): String = {
    options.getOrElse(
      "path", {
        throw AnalysisExceptionFactory.create("'path' is not specified")
      })
  }

}
