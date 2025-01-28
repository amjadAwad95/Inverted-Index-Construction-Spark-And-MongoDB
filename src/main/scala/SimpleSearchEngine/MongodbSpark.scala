package SimpleSearchEngine

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql
import com.mongodb.spark._

class MongodbSpark(
    var appName: String,
    var databaseName: String,
    var collectionName: String
) {

  //create a private data member named spark and its type SparkSession
  private val spark = SparkSession
    .builder()
    .master("local")
    .appName(appName)
    .config(
      "spark.mongodb.read.connection.uri",
      s"mongodb://127.0.0.1/${databaseName}.${collectionName}"
    )
    .config(
      "spark.mongodb.write.connection.uri",
      s"mongodb://127.0.0.1/${databaseName}.${collectionName}"
    )
    .getOrCreate()

  //create a private data member named dataFrame and its type sql.DataFrame
  private var dataFrame: sql.DataFrame = null

  //The function takes as parameter file name and reads and returns RDD(string, string, Array(string))
  def getDataFromFile(fileName: String) = {
    val wordInfoRDD = spark.sparkContext.textFile(fileName)
    wordInfoRDD.map(line ⇒ {
      val info = line.split(",")
      val word = info(0)
      val count = info(1)
      val documentList = info.slice(2, line.length)
      (word, count, documentList)
    })
  }

  //The function takes as a parameter file name and saves the data frame in MongoDB
  def saveDataInMongoDb(fileName: String) = {
    import spark.implicits._
    val wordInfoDataFrame =
      this.getDataFromFile(fileName).toDF("word", "count", "documentList")

    wordInfoDataFrame.write
      .format("mongodb")
      .mode("overwrite")
      .option("database", databaseName)
      .option("collection", collectionName)
      .save()
  }

  //The function returns the data from MongoDB as a data frame
  def getDataFromMongoDb() = {
    dataFrame = spark.read
      .format("mongodb")
      .option("database", databaseName)
      .option("collection", collectionName)
      .load()
    dataFrame
  }

  //The function takes as a parameter word and returns all information about this word
  def getWordInformation(word: String) = {
    import spark.implicits._
    this.dataFrame.filter($"word" === word)
  }

  //The function takes as a parameter word and returns all DocumentList about this word
  def getDocumentListForWord(word: String) = {
    import spark.implicits._
    this.getWordInformation(word).select($"documentList")
  }
}
