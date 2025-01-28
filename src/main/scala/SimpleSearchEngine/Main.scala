package SimpleSearchEngine
import scala.io.StdIn

object Main {
  def main(args: Array[String]): Unit = {
    val wordInfoSpark = new WordInfoSpark("SimpleSearchEngine")
    val wordInfoRDD = wordInfoSpark.getWordInfoRdd("data/HW1/*.txt")
    wordInfoRDD.foreach(println)
//    wordInfoSpark.saveInFile("savedFiles/wholeInvertedIndex.txt")

    val mongodbSpark =
      new MongodbSpark("SimpleSearchEngine", "acme", "wordinfo")

//    mongodbSpark.saveDataInMongoDb("savedFiles/wholeInvertedIndex.txt")
    val mongodbDataFrame = mongodbSpark.getDataFromMongoDb()
//    mongodbDataFrame.show(127)

    println("Enter the word you want to search for in any existing document.")
    val word = StdIn.readLine()

    mongodbSpark
      .getWordInformation(word)
      .show(false)

    mongodbSpark.getDocumentListForWord(word).show(false)
  }

}
