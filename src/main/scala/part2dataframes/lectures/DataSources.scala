package part2dataframes.lectures

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._


//noinspection DuplicatedCode
object DataSources extends App {

  val spark =  SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  // Loading text files
  val lines = spark.read.text("src/main/scala/part1recap/ScalaRecap.scala")
  lines.show(truncate = false)

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // Reading data into a DF requires:
  // - a format
  // - a schema (optional if using the "inferSchema" option)
  // - one of more options

  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") // other modes: dropMalformed, permissive (default)
    .load("src/main/resources/data/cars.json")

  // You can provide the path as an option:
  //  .option("path", "src/main/resources/data/cars.json")
  //  .load()

  // You can specify the format directly:
  //   .json("src/main/resources/data/cars.json")

  // You can pass multiple options at once (useful when computing the options at run-time):
  //   .options(Map(
  //     "inferSchema" -> "true",
  //     "mode" -> "failFast"
  //   ))

  carsDF.show(5)

  // Reading data from a DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  // Writing a DF requires:
  //  - a source DF
  //  - a format
  //  - a save "mode" (overwrite, append, ignore, errorIfExists)
  // - a path
  // - zero or more options
  carsDF.write
    .mode(SaveMode.Overwrite)
    .json("/tmp/data-cars.json")

  // Alternatives:
  //   .format("json")
  //   .option("path", "/tmp/data-cars.json")
  //   .save()

  val stockSchema = StructType(Array(
    StructField("Symbol", StringType),
    StructField("Data", DateType),
    StructField("Price", DoubleType)
  ))

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val stocksDF = spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM dd yyyy")
    .option("header", "true")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.show()

  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("/tmp/data-cars.parquet")

  /** Other formats and their flags:

  Configure compression
    .option("compression", "uncompressed") // other values: bzip2, gzip, lz4, snappy, deflate

  JSON:
    - Specify a date/timestamp format (only works with an enforced schema)
        .option("dataFormat", "YYYY-MM-dd")
      NOTE: if Spark fails to parse the data according to format it will use null as value

    - allow the use single-quotes:
        .option("allowSingleQuotes", "true")

  CSV: pretty difficult to parse because it is so flexible
    - allow header:
      .option("header", "true")
    - specify the separator
      .option("sep", ",")
    - specify the null value
      .option("nullValue", "")

   PARQUET: open-source compressed binary storage format (default storage format for Spark)
    - when calling save() directly it will use parquet as default format

  */
}
