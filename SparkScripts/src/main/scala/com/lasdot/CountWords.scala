package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.collection.immutable.ListMap

object CountWords extends Serializable {
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

    // Get word count: NOTE countBy returns a scala Map() and not an rdd
    val wordCount = rdd.countByValue()

    val mostFrequentWords = ListMap(wordCount.toSeq.sortWith(_._2 > _._2):_*)
    mostFrequentWords.take(100).foreach(println)
  }
}
