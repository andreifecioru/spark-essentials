package part3typesdatasets.lectures

import org.apache.spark.sql.{Encoders, SparkSession}

import java.sql.Date

object BasicDataSets extends App {

  val spark = SparkSession.builder()
    .appName("Basic DataSets - Lecture")
    .config("spark.master", "local")
    .getOrCreate()

  // when dealing with CSV files, enforce the schema while reading to be able to transition to a DS
  val numbersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/numbers.csv")
  numbersDF.printSchema()


  /**
    * DataSets are distributed collections of plain objects. You can use the regular collection processing APIs
    * instead of the DataFrame APIs. There are some drawbacks though: there is no support for execution plan optimizations.
    *
    * If you want big perf. boost, try to use the DataFrame APIs.
    * If you want a type-safe distributed collection (and perf is not the primary concern) you can use the DataSet APIs.
    *
    */

  /**
    * Steps to start working with a DS:
    *  1. Load the data into a DF using the regular Spark data-loading APIs
    *  2. Create the data-model (in the majority of cases this is a plain case-class)
    *  3. Create an Encoder for your data-model (in the majority of cases it is sufficient to 'import spark.implicits._')
    *  4. Transform the DF into a DS
    */

  // STEP 2: Define the data-model
  case class Number(number: Int)

  // STEP 3: bring in the implicit scope an appropriate encoder for out data-model
  // NOTE: we can explicitly define an implicit encoder, but usually we just import spark.implicits._
  // implicit val intEncoder = Encoders.product // all case classes extend Product base trait
  import spark.implicits._

  // STEP 4: transition from the DF API to the DS API
  val numbersDS = numbersDF.as[Number]

  // now we can treat the DS as a distributed collection
  numbersDS.filter(_.number < 1000).show()

  // ---------------[ EXAMPLE 2: Cars DS ]---------------------
  case class Car( /* The field names must match the fields in the JSON (case-sensitive). */
    Name: String,
    Miles_Per_Gallon: Option[Double],  // for columns that have null as potential values
    Cylinders: Long,
    Displacement: Double,
    Horsepower: Option[Long],
    Weight_in_lbs: Long,
    Acceleration: Double,
    Year: String,
    Origin: String
  )

  val carsDS = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json").as[Car]

  carsDS.map(_.Name).show()


}
