package part2dataframes.exercises

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Joins extends App {

  def loadTable(dbTable: String): DataFrame = {
    spark.read.format("jdbc")
      .options(readOptions + ("dbtable" -> s"public.$dbTable"))
      .load().persist()
  }

  def mostRecent(df: DataFrame) = {
    df.groupBy("emp_no")
      .agg(
        max("to_date").as("max_to_date"),
      )
      .withColumnRenamed("emp_no", "emp_id")
      .join(df, expr("max_to_date = to_date and emp_id = emp_no"))
      .drop("emp_id")
  }

  val spark = SparkSession.builder()
    .appName("Join - Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  // Load the data from the DB
  val readOptions = Map(
    "format" -> "jdbc",
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker"
  )

  val employeesDF = loadTable("employees")
  val salariesDF = loadTable("salaries")
  val titlesDF = loadTable("titles")
  val managersDF = loadTable("dept_manager")

  // 1. Show all employees and their max salary
  val maxSalariesDF = salariesDF.selectExpr("emp_no", "salary")
    .groupBy("emp_no")
    .max("salary")
    .withColumnRenamed("max(salary)", "max_salary")

  employeesDF.join(maxSalariesDF, "emp_no")
//    .show()

  // 2. Show the employees who have never been managers
  val notManagersDF = employeesDF.join(managersDF, employeesDF.col("emp_no") === managersDF.col("emp_no"), "anti")
//  notManagersDF.show()

  println(s"No. of employees: ${employeesDF.count()}")
  println(s"No. of non-managers: ${notManagersDF.count()}")

  // 3. Find the job titles of the best paid 10 employees
  val highestSalariesDF = mostRecent(salariesDF)
    .orderBy(desc("salary"))
    .limit(10)

  val latestTitlesDF = mostRecent(titlesDF)

  latestTitlesDF.join(highestSalariesDF, "emp_no")
    .select("emp_no", "title", "salary")
    .orderBy(desc("salary"))
    .show()
}
