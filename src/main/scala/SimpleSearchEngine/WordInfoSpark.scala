package SimpleSearchEngine

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import java.io._

class WordInfoSpark(var appName: String) {
  //To remove the log
  private val nullAppender = new NullAppender
  BasicConfigurator.configure(nullAppender)

  //Create SparkConf
  private val sparkConf =
    new SparkConf()
      .setMaster("local[*]")
      .setAppName(appName)

  //make a spark context
  private val sc = new SparkContext(sparkConf)
  //create a private data member named data and its type RDD[String]
  private var data: RDD[String] = sc.parallelize(List("default"))

  //The function takes as parameter file name and reads and returns RDD[(string,string)]
  private def readFiles(fileName: String) = {
    sc.wholeTextFiles(("data/HW1/*.txt"))
  }

  ////The function takes as parameter file name and get words information
  def getWordInfoRdd(fileName: String) = {
    val textRDD = this.readFiles(fileName)
    data = textRDD
      .map(pair ⇒
        (
          (
            pair._1.split("/").last,
            pair._2
              .replace(",", "")
              .replace(".", "")
              .replace("!", "")
              .replace("?", "")
              .replace("\r\n", "")
          )
        )
      )
      .flatMapValues(string ⇒ string.split(" "))
      .map(pair ⇒ (pair._2, (1, pair._1)))
      .reduceByKey((x, y) ⇒ {
        (x._1 + y._1, x._2 + " " + y._2)
      })
      .map(pair ⇒ {
        var set = scala.collection.mutable.Set[String]()
        val fileNameArray = pair._2._2.split(" ").sorted
        fileNameArray
          .foreach(name ⇒ set += name.replace("txt", "").replace(".", ""))
        var sortedName: Array[String] = set.toArray.sorted
        s"${pair._1},${pair._2._1},${sortedName.mkString(",")}"
      })
      .sortBy(x ⇒ x, ascending = true)
      .filter(item ⇒ item.split(",")(0) != "")
    data
  }

  ////The function takes as parameter file name and save the rdd in file
  def saveInFile(fileName: String) = {
    val writer = new PrintWriter(new File(fileName))
    data.collect().foreach(line => writer.println(line))
    writer.close()
  }
}
