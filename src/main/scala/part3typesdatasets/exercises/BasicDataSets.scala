package part3typesdatasets.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object BasicDataSets extends App {

  val spark = SparkSession.builder()
    .appName("Basic Data Sets - Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  case class Car(
    Name: String,
    Miles_Per_Gallon: Option[Double],
    Cylinders: Long,
    Displacement: Double,
    Horsepower: Option[Long],
    Weight_in_lbs: Long,
    Acceleration: Double,
    Year: String,
    Origin: String
  )

  val carsDS = carsDF.as[Car]

  // 1. Count how many cars we have
  println(s"We have ${carsDS.count()} cars")

  // 2. Count how many POWERFUL cars we have (HP > 140)
  val powerfulCarsDS = carsDS.flatMap(_.Horsepower).filter(_ > 140)
  println(s"We have ${powerfulCarsDS.count()} powerful cars")

  // 3. Average HP for the entire data-set
  val totalHP = carsDS.flatMap(_.Horsepower).reduce(_ + _)
  println(s"Total HP: $totalHP")
  println(s"Avg. HP: ${totalHP.toDouble / carsDS.count()}")

  // ... same as (we can still use select - gets us back to DF API land)
  // all data-frames are data-sets of Row's
  carsDS.select(avg($"Horsepower")).show()
}
