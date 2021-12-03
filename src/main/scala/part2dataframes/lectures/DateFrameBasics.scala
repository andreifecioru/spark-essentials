package part2dataframes.lectures

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._


//noinspection DuplicatedCode
object DateFrameBasics extends App {

  // create a SparkSession instance
  val spark = SparkSession.builder()
    .appName("DataFrame Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // load JSON-formatted data
  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // show the schema
  carsDF.printSchema()

  // show the actual data (a subset)
  carsDF.show(truncate = false)

  // a DF is a schema + a distributed collection of rows that follow the schema
  carsDF.take(10).foreach(println)

  // Spark types: enforced on a DF at run-time
  val longType = LongType

  // Spark supports nested types
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

  // NOTE: in practice we should enforce a schema explicitly
  // rather than replying on the "inferSchema" option

  // obtain a schema of an existing DF
  val carsSchema1 = carsDF.schema
  println(carsSchema1)

  // read a DF with an explicit schema
  val carsDFWithSchema = spark.read
    .schema(carsSchema)
    .json("src/main/resources/data/cars.json")

  carsDFWithSchema.show()

  // you can create a row from arbitrary pieces of data
  val car = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // you can create a DF from tuples
  val carsAsTuples = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(carsAsTuples) // schema is known at compile-time (but no column names)


  // NOTE: Row's don't have schemas - schemas are only a DF concept

  // create DFs with implicits
  import spark.implicits._
  val manualCarsWithImplicits = carsAsTuples.toDF("Name", "Miles_Per_Gallon", "Cylinders",
                                                  "Displacement", "Horsepower", "Weight_in_lbs",
                                                  "Acceleration", "Year", "Origin")
  manualCarsWithImplicits.show()
}
