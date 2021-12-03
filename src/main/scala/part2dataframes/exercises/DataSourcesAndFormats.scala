package part2dataframes.exercises

import org.apache.spark.sql.{SaveMode, SparkSession}


object DataSourcesAndFormats extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats - Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.show()

  println("Saving as CSV")
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .csv("/tmp/movies.csv")

  println("Saving as PARQUET")
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("compression", "snappy")
    .parquet("/tmp/movies.parquet")

  println("Saving in DB")
  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .mode(SaveMode.Overwrite)
    .save()

}
