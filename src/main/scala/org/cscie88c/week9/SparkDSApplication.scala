package org.cscie88c.week9

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.cscie88c.config.{ConfigUtils}
import org.cscie88c.utils.{SparkUtils}
import org.apache.spark.sql.{SparkSession, Dataset, Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

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

    // Read the CSV file into a Dataset[RawTransaction]
    val rawTransactions = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(conf.transactionFile)
      .as[RawTransaction]

    // Convert RawTransaction to CustomerTransaction
    val customerTransactions =
      rawTransactions.map(raw => CustomerTransaction(raw))

    customerTransactions
  }

  def transactionTotalsByCategory(
      spark: SparkSession,
      transactions: Dataset[CustomerTransaction]
  ): Dataset[(String, Double)] = {
    import spark.implicits._

    val sumAggregator = new Aggregator[CustomerTransaction, Double, Double] {
      def zero: Double = 0.0
      def reduce(b: Double, a: CustomerTransaction): Double =
        b + a.transactionAmount
      def merge(b1: Double, b2: Double): Double = b1 + b2
      def finish(reduction: Double): Double = reduction
      override def bufferEncoder: Encoder[Double] = Encoders.scalaDouble
      override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }.toColumn

    // Group by category and calculate total
    val result = transactions
      .groupByKey(t => if (t.transactionAmount > 80) "High" else "Standard")
      .agg(sumAggregator)

    result
  }

  def printTransactionTotalsByCategory(ds: Dataset[(String, Double)]): Unit = {
    // Collect the dataset to the driver node as an Array
    val categoryTotals = ds.collect()

    // Iterate through the array and print each category and its total
    categoryTotals.foreach { case (category, total) =>
      println(s"Category: $category, Total: $total")
    }
  }

}
