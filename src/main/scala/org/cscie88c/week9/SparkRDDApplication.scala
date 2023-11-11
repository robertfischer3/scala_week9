package org.cscie88c.week9

import org.apache.spark.sql.SparkSession
import org.cscie88c.utils.{SparkUtils}
import org.apache.spark.rdd.{RDD}
import pureconfig.generic.auto._
import pureconfig._

// write case class below
case class SparkRDDConfig(
    name: String,
    masterUrl: String,
    transactionFile: String
)

// run with: sbt "runMain org.cscie88c.week9.SparkRDDApplication"
object SparkRDDApplication {

  // application entry point
  def main(args: Array[String]): Unit = {
    implicit val conf: SparkRDDConfig = readConfig() // 1. read configuration
    println(conf)

    val spark = SparkUtils.sparkSession(
      conf.name,
      conf.masterUrl
    ) // 2. initialize spark session
    val rddLines = loadData(spark) // 3.load data
    val rddTransactions = lineToTransactions(
      rddLines
    ) // 4. convert lines to transaction objects
    val yearlyTransactionsRDD = transactionsAmountsByYear(
      rddTransactions
    ) // 5. transform data

    printTransactionsAmountsByYear(yearlyTransactionsRDD) // 6. print results

    spark.stop() // 7. stop spark cluster

  }

  def readConfig(): SparkRDDConfig = {

    val conf: SparkRDDConfig = ConfigSource.default
      .at("org.cscie88c.spark-rdd-application")
      .loadOrThrow[SparkRDDConfig]

    conf
  }

  def loadData(
      spark: SparkSession
  )(implicit conf: SparkRDDConfig): RDD[String] = {
    spark.sparkContext.textFile(conf.transactionFile)
  }

  def lineToTransactions(lines: RDD[String]): RDD[CustomerTransaction] = {

    val filteredLines = lines.filter(_ != "customer_id,trans_date,tran_amount")

    filteredLines.collect {

      case line if !line.isEmpty =>
        val parts = line.split(",")

        val date = parts(1)
        val datePattern = """(\d{2})-([a-zA-Z]{3})-(\d{2})""".r

        date match {
          case datePattern(_, _, _) =>
            CustomerTransaction(parts(0), date, parts(2).toDouble)
        }
    }

  }

  def transactionsAmountsByYear(
      transactions: RDD[CustomerTransaction]
  ): RDD[(String, Double)] = {

    transactions
      .map { transaction =>
        val year = transaction.transactionDate.split("-")(2)
        (year, transaction.transactionAmount)
      }
      .reduceByKey(_ + _)
  }

  def printTransactionsAmountsByYear(
      transactions: RDD[(String, Double)]
  ): Unit = {
    val results = transactions.collect()

    println("Transactions amounts by year:")

    results.foreach { case (year, amount) =>
      println(s"Year $year: Sum Transactions: $amount")
    }
  }
}
