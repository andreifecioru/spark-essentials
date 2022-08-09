package part3typesdatasets.lectures

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object AdvancedDataSets extends App {

  val spark = SparkSession.builder()
    .appName("Advanced Data Sets - Lecture")
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

  // ---------------[ JOINS ]---------------
  // NOTES:
  //   - use the "joinWith" APIs - the "join" APIs return a DataFrame (not Dataset)
  //   - the return type of the join is a data-set of a tuple of the types of the data-sets involved in the join operation

  val joinedDS: Dataset[(Player, Band)] =
    playerDS.joinWith(bandDS, playerDS.col("band") === bandDS.col("id"))

  joinedDS
    .withColumnRenamed("_1", "Player")
    .withColumnRenamed("_2", "Band")
    .show()

  // ----------------[ GROUPING ]-------------
  // NOTES:
  //  - use "groupByKey" APIs - the "groupBy" APIs return a DataFrame (not a Dataset)
  //  - the return type of a group operation is a KeyValueGroupedDataset[K, V], after which we must perform an
  //    aggregation to get back a data-set
  bandDS
    .groupByKey(_.hometown)
    .count()  // we must call an aggregation now; this returns a Dataset[(String, Long)]
    .show

}
