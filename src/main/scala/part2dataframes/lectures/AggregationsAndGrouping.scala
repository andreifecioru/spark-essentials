package part2dataframes.lectures

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object AggregationsAndGrouping extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping - Lecture")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // moviesDF.show(5)

  // 1. Aggregations
  // count all values including null
  moviesDF.select(count($"*"))//.show()
  moviesDF.selectExpr("count(*)")//.show()
  // same as: moviesDF.count

  // count all values except null
  moviesDF.select(count($"Major_Genre"))//.show()
  moviesDF.selectExpr("count(Major_Genre)")//.show()

  // counting distinct values
  moviesDF.select(countDistinct($"Major_Genre"))//.show()
  moviesDF.selectExpr("approx_count_distinct(Major_Genre)")//.show()

  // min / max
  moviesDF.select(min($"IMDB_Rating"))//.show()
  moviesDF.selectExpr("max(IMDB_Rating)")//.show()

  // sum / avg / mean/ std-dev
  moviesDF.select(sum($"US_Gross"))//.show()
  moviesDF.selectExpr("avg(US_Gross)")//.show()

  moviesDF.select(
    mean($"IMDB_Rating"),
    expr("stddev(IMDB_Rating)")
  )//.show()

  // 2. Grouping

  moviesDF.groupBy($"Major_Genre") // this returns a "relational grouped data-set"
    // to get back a DF you must perform an aggregation
    .count()
    // then you can perform an action
    //.show()

  moviesDF
    .groupBy($"Major_Genre")
    .avg("IMDB_Rating")

  // alternative way to do aggregations
  moviesDF
    .groupBy($"Major_Genre")
    .agg (
      count("*").as("MovieCount"),
      avg("IMDB_Rating").as("AvgRating")
    )
    .orderBy("AvgRating")
    .show()


}
