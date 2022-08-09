package part3typesdatasets.lectures

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls - Lecture")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Select the 1st non-null value from a list of columns
  moviesDF.select(
    $"Title",
    coalesce($"Rotten_Tomatoes_Rating", $"IMDB_Rating" * 10).as("Rating") // choose the 1st non-null value
  )
//    .show()

  // Checking for nulls
  moviesDF.where($"IMDB_Rating".isNotNull)

  // Handling null values while sorting
  moviesDF.orderBy($"IMDB_Rating".desc_nulls_last) // other options: desc_nulls_first, asc_nulls_first, asc_nulls_last

  // Ops with null values with the "na" object

  // Drop all the rows that contain null in any of the cols
  moviesDF.select("Title", "IMDB_Rating").na.drop()

  // Replace null values with default values

  // Search for null in a list of cols and replace with a default value
  moviesDF.na.fill(0, Seq("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  // .. with even more flexibility (when dealing with cols of different types)
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Director" -> "Unknown"
  ))

  // more ops with nulls (available ONLY inside "expr" constructs)
  moviesDF.selectExpr(
    "Title", "Rotten_Tomatoes_Rating", "IMDB_Rating",

    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating) as ifnull",    // if (1st == null) then 2nd
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating) as nvl",          // same as ^^
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating) as nullif",    // if (1st == 2nd) then null else 1st
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating, 0.0) as nlv2",   // if (1st != null) 2nd else 3rd
  )
  .show()
}
