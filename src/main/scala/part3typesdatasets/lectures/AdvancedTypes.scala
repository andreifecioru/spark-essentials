package part3typesdatasets.lectures

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AdvancedTypes extends App {

  val spark = SparkSession.builder()
    .appName("Advanced Types - Lecture")
    .config("spark.master", "local")
    .getOrCreate()

  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

  import spark.implicits._

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // --------------[ Working with dates ]------------------------
  // NOTE: when parsing fails, Spark generates null as value
  val moviesWithDatesDF = moviesDF.select(
    $"Title",
    to_date($"Release_Date", "dd-MMM-yy").as("Released_On")
  )
  .where($"Release_Date".isNotNull)

  // Other date-related APIs
  moviesWithDatesDF
    .withColumn("Today", current_date)     // get today's date
    .withColumn("Now", current_timestamp)  // get current Unix time-stamp (in secs)
    // compute date-difference (in days)
    .withColumn("Days_Since_Release", datediff($"Today", $"Released_On"))

  // Dealing with multiple formats:
  //  - parse the DF multiple times
  //  - union the resulting smaller DFs

  val dateFormats = Seq("dd-MMM-yy", "yyyy-MM-dd")

  dateFormats.map { fmt =>
    moviesDF.select(
      $"Title", $"Release_Date",
      to_date($"Release_Date", fmt).as("Released_On")
    )
    .where($"Released_On".isNotNull)
  }
  .reduce(_ union _)
//  .show()


  // ---------------------[ Struct columns ]---------------------
  // You can create a struct-col from multiple existing cols
  val moviesWithProfitDF = moviesDF.select(
    $"Title",
    struct($"US_Gross", $"Worldwide_Gross", $"US_DVD_Sales").as("Profit")
  )

  // You can extract individual fields from struct-cols
  moviesWithProfitDF.select(
    $"Title",
    $"Profit".getField("US_Gross").as("US_Profit")
  )

  // Same can be achieved with "expr"
  moviesDF
    // creation
    .selectExpr("Title", "(US_Gross, Worldwide_Gross, US_DVD_Sales) as Profit")
    // field extraction
    .selectExpr("Title", "Profit.US_Gross")
//    .show()


  // ---------------------[ Array columns ]----------------------
  val moviesWithArrayColDF = moviesDF.select(
    $"Title",
    split($"Title", " |,").as("Title_Words")  // split based on reg-ex separator -> returns an array
  )

  moviesWithArrayColDF.select(
    $"Title",
    expr("Title_Words[0]"),   // extraction by index
    size($"Title_Words"),
    array_contains($"Title_Words", "Love")
  )
  .show()
}
