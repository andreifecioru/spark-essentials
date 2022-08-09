package part2dataframes.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("Columns and Expressions - Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  moviesDF.show(5)

  moviesDF.selectExpr("Title", "Major_Genre").show(5)

  moviesDF
    .selectExpr(
      "Title",
      "(Worldwide_Gross + US_DVD_Sales + US_Gross) as TotalProfit"
    )
    .where("TotalProfit is not null")
    .show(5)

  moviesDF
    .selectExpr("Title", "Major_Genre", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6 and true and Major_Genre is not null")
    .show(5)

}
