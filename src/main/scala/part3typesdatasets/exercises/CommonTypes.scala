package part3typesdatasets.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common Types - Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val carsDF = spark.read.json("src/main/resources/data/cars.json")
  val carNames = List("Ford", "Volkswagen", "VW")

  // Solution 1: with "contains"
  val filterExpr = carNames
    .map(_.toLowerCase)
    .map(name => $"Name".contains(name))
    .foldLeft(lit(false))(_ or _)

  carsDF
    .where(filterExpr)
    .show()

  // Solution 2: with reg-ex
  val pattern = carNames.map(_.toLowerCase).mkString("|")
  carsDF
    .withColumn("pattern", regexp_extract($"Name", pattern, 0))
    .where($"pattern" =!= "")
    .drop("pattern")
    .show()
}
