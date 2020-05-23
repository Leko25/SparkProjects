package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

//noinspection DuplicatedCode
object CountWordsRDD extends Serializable {
  def wordFilter(txt: String): Boolean = {
    var filterFlag = false
    val pattern = "[a-zA-Z]+"
    if (txt.matches(pattern)) {
      filterFlag = true
    }
    filterFlag
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CountWords")

    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/book.txt")

    // Get each individual word and filter only actual words
    val rdd = lines.flatMap(line => line.split(" ")).filter(wordFilter)

    // Get word count and return RDD
    val wordCount = rdd.map(word => (word, 1)).reduceByKey((x, y) => x + y).sortBy(_._2, ascending = false)

    val mostFrequentWords = wordCount.collect()
    mostFrequentWords.take(100).foreach(println)
  }
}
