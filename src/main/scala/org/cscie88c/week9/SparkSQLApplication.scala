package org.cscie88c.week9

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{LazyLogging}
import org.cscie88c.config.{ConfigUtils}
import org.cscie88c.utils.{SparkUtils}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import pureconfig.generic.auto._
import pureconfig._
import org.apache.spark.sql.functions.{when}
import org.apache.spark.sql.functions.{col, when}

// run with: sbt "runMain org.cscie88c.week9.SparkSQLApplication"
object SparkSQLApplication {

  def main(args: Array[String]): Unit = {
    implicit val conf: SparkDSConfig = readConfig()
    val spark = SparkUtils.sparkSession(conf.name, conf.masterUrl)
    val transactionDF = loadData(spark)
    val augmentedTransactionsDF = addCategoryColumn(transactionDF)
    augmentedTransactionsDF.createOrReplaceTempView("transactions")
    val sparkSQL = """
              SELECT category, SUM(tran_amount) AS total_amount
              FROM transactions
              GROUP BY category
            """
    val totalsByCategoryDF = spark.sql(sparkSQL)
    printTransactionTotalsByCategory(totalsByCategoryDF)
    spark.stop()
  }

  def readConfig(): SparkDSConfig = {

    val conf: SparkDSConfig = ConfigSource.default
      .at("org.cscie88c.spark-ds-application")
      .loadOrThrow[SparkDSConfig]

    conf
  }

  def loadData(spark: SparkSession)(implicit conf: SparkDSConfig): DataFrame = {
    // Read the CSV file
    val df = spark.read
      .option("header", "true") // assuming the file has a header
      .option("inferSchema", "true") // lets Spark infer the schema
      .csv(conf.transactionFile)

    df
  }

  def addCategoryColumn(raw: DataFrame): DataFrame = {
    // Add a new column 'category' based on the condition
    val categorizedDF = raw.withColumn(
      "category",
      when(col("tran_amount") > 80, "High").otherwise("Standard")
    )

    categorizedDF
  }

  def printTransactionTotalsByCategory(df: DataFrame): Unit = {
    println("Transaction totals by category:")
    df.collect().foreach { row =>
      val category = row.getAs[String]("category")
      val totalAmount =
        row.getAs[Long]("total_amount") // Changed from Double to Long
      println(s"Category: $category, Total Amount: $totalAmount")
    }
    println(df)

  }

}
