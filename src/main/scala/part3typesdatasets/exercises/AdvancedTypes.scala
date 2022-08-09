package part3typesdatasets.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object AdvancedTypes extends App {

  val spark = SparkSession.builder()
    .appName("Advanced Types - Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val stocksDF = spark.read
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF
    .withColumn("date_date", to_date($"date", "MMM d yyyy"))
    .drop("date")
    .withColumnRenamed("date_date", "date")
    .show()

}
