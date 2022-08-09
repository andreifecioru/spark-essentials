package part3typesdatasets.lectures

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Types - Lectures")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // add literal values as a new column to DF
  moviesDF.select($"Title", lit(47).as("literal_value"))
//    .show()

  // add dynamically computed values as columns
  // frequent use-case: compute a predicate based on some cols, and add it as a new col
  val goodDramasFilter = $"Major_Genre" === "Drama" and $"IMDB_Rating" > 7.0
  val goodDramasDF = moviesDF.select($"Title", goodDramasFilter.as("good_drama"))

//  goodDramasDF.show()

  // since the "good_drama" col contains boolean values, we can use it's name as a filter expression
  goodDramasDF.select("Title")
    .where("good_drama")
//    .where(not($"good_drama"))
//    .show()

  // For cols with numerical values, we can use most of the usual math ops
  val avgRating = expr("(Rotten_Tomatoes_Rating / 10 + IMDB_Rating) / 2")

  moviesDF.select($"Title", avgRating.as("avg_rating"))
    .where($"avg_rating" >= 0.0)
//    .show()

  // computing the correlation between 2 cols
  val correlation = moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") // this is an action, just like count
  println(correlation)

  // String ops: initcap, lower, upper, contains and reg-ex matching
  carsDF.select(initcap($"Name"))//.show()
  carsDF.select($"Name", $"Name".contains("volkswagen").as("is_vw"))//.show()

  // reg-ex ops
  val pattern = "volkswagen|vw"
  // 1. extract data from matching groups
  carsDF.select(
    $"Name",
    regexp_extract($"Name", pattern, 0).as("pattern") // extract the 1st match group
  )
  .where($"pattern" =!= "")
//  .show()

  // 2. replace data from matching groups
  carsDF.select(
    $"Name",
    regexp_replace($"Name", pattern, "People's car").as("pattern")
  )
  .where($"pattern".contains("People"))
  .show()
}
