package org.cscie88c.week9

import org.cscie88c.utils.{SparkUtils}
import org.apache.spark.sql.{SparkSession, Dataset}

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

    // Group by category
    val grouped = transactions
      .groupByKey(t => if (t.transactionAmount > 80) "High" else "Standard")

    // Use reduceGroups to sum the transaction amounts
    val result = grouped
      .reduceGroups((t1, t2) =>
        CustomerTransaction(
          t1.customerId,
          t1.transactionDate,
          t1.transactionAmount + t2.transactionAmount
        )
      )
      .map { case (category, transaction) =>
        (category, transaction.transactionAmount)
      }

    result
  }

  def printTransactionTotalsByCategory(ds: Dataset[(String, Double)]): Unit = {

    // Collect the dataset to the driver node as an Array
    val categoryTotals = ds.collect()

    println("Transaction totals by category:")

    // Iterate through the array and print each category and its total
    categoryTotals.foreach { case (category, total) =>
      println(s"Category: $category, Total: $total")
    }
  }

}
