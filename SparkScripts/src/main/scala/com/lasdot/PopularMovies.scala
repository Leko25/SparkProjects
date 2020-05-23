package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object PopularMovies {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMovies")

    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/ml-100k/u.data")

    val rdd = lines.map(line => (line.split("\t")(1), 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)

    val results = rdd.collect()

    results.take(100).foreach(println)
  }
}
