package part3typesdatasets.exercises

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._


object AdvancedDataSets extends App {

  val spark = SparkSession.builder()
    .appName("Advanced Data Sets - Exercises")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  def loadFromJsonFile(fileName: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$fileName")
  }

  // Data model classes
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class Player(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarDS = loadFromJsonFile("guitars.json").as[Guitar]
  val playerDS = loadFromJsonFile("guitarPlayers.json").as[Player]
  val bandDS = loadFromJsonFile("bands.json").as[Band]

  val resultDS = playerDS.joinWith(
    guitarDS,
    array_contains(playerDS.col("guitars"), guitarDS.col("id")),
    "outer")

  resultDS.show()

}
