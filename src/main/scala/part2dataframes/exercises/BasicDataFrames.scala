package part2dataframes.exercises

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.util.UUID

object BasicDataFrames extends App {

  val spark = SparkSession.builder()
    .appName("DataFrames - Exercise 001")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val phoneData = Seq(
    ("Motorola", "Droid", 12, "300x200"),
    ("Samsung", "Galaxy", 18, "800x600"),
    ("Samsung", "Galaxy", 18, "800x600"),
    ("Samsung", "Galaxy", 18, "800x600"),
    ("Samsung", "Galaxy", 18, "800x600"),
    ("Apple", "iPhone-6", 10, "1020x768")
  )

  val uuidUDF = udf(() => UUID.randomUUID().toString)
  val phonesDF = phoneData.toDF("Manufacturer", "Model", "CameraMP", "Resolution")
    .withColumn("uuid", uuidUDF())
  phonesDF.printSchema()
  phonesDF.show()

//  val moviesDF = spark.read
//    .option("inferSchema", "true")
//    .json("src/main/resources/data/movies.json")
//
//  moviesDF.printSchema()
//  moviesDF.show()
//  println(s"We have ${moviesDF.count} movies.")

}
