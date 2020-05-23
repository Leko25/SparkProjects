package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object ReviewsCount {
  def main(args: Array[String]): Unit = {
    // Set Error Logs to avoid spamming
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named ReviewsCount
    val sc = new SparkContext("local[*]", "ReviewsCount")

    //Load each line of the ratings data into an RDD
    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/ml-100k/u.data")

    /*
      Convert each line to a string, split on tabs and extract the third field (rating).
      (The file format is [userID, movieID, rating, timestamp]
     */
    val ratings = lines.map(line => line.split("\t")(2))

    // Count how many times each value rating occurs
    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    sortedResults.foreach(println)
  }
}
