package org.cscie88c.week9

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.cscie88c.config.{ConfigUtils}
import org.cscie88c.utils.{SparkUtils}
import org.apache.spark.sql.{Dataset}

import pureconfig.generic.auto._
import pureconfig._

// write config case class below
case class SparkDSConfig(
    name: String,
    masterUrl: String,
    transactionFile: String
)

// run with: sbt "runMain org.cscie88c.week9.SparkDSApplication"
object SparkDSApplication {

  // application main entry point
  def main(args: Array[String]): Unit = {
    implicit val conf: SparkDSConfig = readConfig()
    val spark = SparkUtils.sparkSession(conf.name, conf.masterUrl)
    val transactionDS = loadData(spark)
    val totalsByCategoryDS = transactionTotalsByCategory(spark, transactionDS)
    printTransactionTotalsByCategory(totalsByCategoryDS)
    spark.stop()
  }

  def readConfig(): SparkDSConfig = {

    val conf: SparkDSConfig = ConfigSource.default
      .at("org.cscie88c.spark-ds-application")
      .loadOrThrow[SparkDSConfig]

    conf
  }

  def loadData(
      spark: SparkSession
  )(implicit conf: SparkDSConfig): Dataset[CustomerTransaction] = {
    import spark.implicits._

    spark.read
      .option("header", "true")
      .csv(conf.transactionFile)
      .as[CustomerTransaction]
  }

  def transactionTotalsByCategory(
      spark: SparkSession,
      transactions: Dataset[CustomerTransaction]
  ): Dataset[(String, Double)] = {

    import spark.implicits._

    transactions
      .as[CustomerTransaction]
      .map(t => (getCategory(t), t.transactionAmount))
      .groupByKey(_._1)
      .reduceGroups((c1, c2) => (c1._1, c1._2 + c2._2))
      .map { case (category, total) => (category, total) }

  }

  def getCategory(t: CustomerTransaction): String = {
    if (t.transactionAmount > 80) "High" else "Standard"
  }

  def printTransactionTotalsByCategory(ds: Dataset[(String, Double)]): Unit =
    ???
}
