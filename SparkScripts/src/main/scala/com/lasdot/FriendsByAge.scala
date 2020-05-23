package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge {
  def main(args: Array[String]): Unit = {
    // Set Log Level to Error types only
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Get Spark Context and use all cores on local machine, name each process FriendsByAge
    val sc = new SparkContext("local[*]", "FriendsByAge")

    // Read each line of source data
    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/fakefriends.csv")

    // Convert data to map (age, num_friends)
    val rdd = lines.map(line => (line.split(",")(2).toInt, line.split(",")(3).toInt))
    //rdd.take(4).foreach(println)

    // Map values to (age, (num_friends, 1)), then sum the tuple values for each key
    val ageToSum = rdd.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

    // Find the average friends for each key
    val avgByAge = ageToSum.mapValues(x => x._1/x._2)

    // Collect the results from the RDD, which initiates the DAG and executes the job
    val results = avgByAge.collect()

    results.sorted.foreach(println)
  }
}
