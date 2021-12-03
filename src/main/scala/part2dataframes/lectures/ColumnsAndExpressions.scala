package part2dataframes.lectures

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("Columns and Expressions - Lecture")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read.json("src/main/resources/data/cars.json")
  carsDF.show(5)

  // Columns - objects that allow us to generate new DFs via "projections"
  val nameColumn = carsDF.col("Name")
  val carNamesDF = carsDF.select(nameColumn)
  carNamesDF.show(5)

  import spark.implicits._
  // Alternative methods for generating projections
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"), // same as carsDF.col()
    column("Weight_in_lbs"),
    'Year,         // Symbol auto-converted to column
    $"Horsepower", // Same as above ^^
    expr("Origin") // EXPRESSION - powerful concept
  )

  // you can pass in the names of the cols as strings
  carsDF.select("Name", "Year", "Origin") // cannot be mixed with the methods above

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Name")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  carsDF.select(
    'Name,
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  ).show(5)

  // select-expr projections
  carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as Weight_in_kg"
  ).show(5)

  // DF processing

  // 1. Adding a new column
  carsDF.withColumn("Weight_in_kg", weightInKgExpression)
    .select("Name", "Weight_in_kg")
    .show(5)

  // 2. Rename a column
  val carsWithColRenamedDF = carsDF
    .withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // NOTE: careful when using col names in expressions
  carsWithColRenamedDF
    // use back-ticks when dealing with column names which would break the expression compiler
    .selectExpr("Name","`Weight in pounds`")
    .show(5)

  // 3. Remove a column
  carsDF.drop("Cylinders", "Displacement").show(5)

  // 4. Filtering
  carsDF.filter($"Origin" =!= "USA").show(5)
  carsDF.where($"Origin" === "USA").show(5) // where is an alias for filter

  // 4.1 Filtering with expression string
  carsDF.where("Origin = 'USA'").show(5)

  // 4.2 Chaining filters
  carsDF.where($"Origin" === "USA").where($"Horsepower" > 150).show(5)
  carsDF.where($"Origin" === "USA" and $"Horsepower" > 150).show(5)
  carsDF.where("Origin = 'USA' and Horsepower > 150").show(5)

  // 5. Adding more rows with union
  val moreCarsDF = spark.read.json("src/main/resources/data/more_cars.json")

  // NOTE: union works only if both DFs have the same schema
  val allCarsDF = carsDF union moreCarsDF

  // 6. Distinct values
  allCarsDF.select("Origin").distinct().show()

}
