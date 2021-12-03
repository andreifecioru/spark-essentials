package part2dataframes.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object GroupingAndAggregations extends App {

  val spark = SparkSession.builder()
    .appName("Grouping and Aggregations - Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDB = spark.read.json("src/main/resources/data/movies.json")

  // 1. Sum up all the profits of all the movies
  moviesDB
    .selectExpr("US_Gross + US_DVD_Sales + Worldwide_Gross as Profit")
    .where("Profit is not null")
    .selectExpr("sum(Profit) as TotalProfit")
    .show()

  // 2. Count how many distinct non-null directors we have
  moviesDB
    .where("Director is not null")
    .select(countDistinct("Director").as("DirectorCount"))
    .show()

  // 3. Show the mean and std-dev for US_Gross revenue
  moviesDB
    .selectExpr(
      "mean(US_Gross) as MeanGrossUS",
      "stddev(US_Gross) as StdDevGrossUS"
    )
    .show()

  // 4. Compute the avg IMDB rating and avg US_Gross revenue per director
  moviesDB
    .where("Director is not null and IMDB_Rating is not null")
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("AvgRating"),
      sum("US_Gross").as("GrossUS")
    )
    .orderBy($"AvgRating".desc)
    .show()

}
