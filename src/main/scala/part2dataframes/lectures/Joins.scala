package part2dataframes.lectures

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins - Lecture")
    .config("spark.master", "local")
    .getOrCreate()

  val playersDF = spark.read.json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.json("src/main/resources/data/bands.json")
  val guitarsDF = spark.read.json("src/main/resources/data/guitars.json")

  // JOINS - they are wide operations (i.e. expensive)

  val joinCondition = bandsDF.col("id") === playersDF.col("band")

  // 1. INNER JOIN - keep only the data that is present in both DFs
  playersDF.join(bandsDF, joinCondition, "inner")  // "inner" is the default join type
//    .show()

  // 2. OUTER JOIN - keep all the data from both DFs and put null where the data is missing
  playersDF.join(bandsDF, joinCondition, "outer")
//    .show()

  // 3. LEFT OUTER JOIN - keep all the data from the left DF and put null where the data is missing
  playersDF.join(bandsDF, joinCondition, "left_outer")
//    .show()

  // 4. RIGHT OUTER JOIN - keep all the data from the right DF and put null where the data is missing
  playersDF.join(bandsDF, joinCondition, "right_outer")
//    .show()

  // 5. SEMI-JOIN - keep the columns only from the left DF for which the condition holds
  playersDF.join(bandsDF, joinCondition, "semi")//.show() // available aliases: left_semi, leftsemi

  // 6. ANTI-JOIN - keep the columns only from the left DF for which the condition DOES NOT hold
  playersDF.join(bandsDF, joinCondition, "anti")//.show()

  // NOTES:

  // 1. Duplicate columns: when we do a full join we can see the we get 2 cols with the same name "id"
  //    This is why the following crashes:

  //  playersDF.join(bandsDF, joinCondition).select("id").show()

  // Solutions:
  // Rename the offending column
  playersDF.join(bandsDF.withColumnRenamed("id", "band"), "band")
    //    .select("id")
//    .show()

  // Rename the offending column from one of the DFs and keep bot columns
  playersDF.join(bandsDF.withColumnRenamed("id", "bandId"), expr("band == bandId"))
    .select("id")
  //    .show()


  // Drop the offending column
  playersDF.join(bandsDF, joinCondition)
    .drop(bandsDF.col("id")) // this works because Spark keeps and internal unique ID for each col
    .show()
}
