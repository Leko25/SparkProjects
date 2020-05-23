package com.lasdot

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

/*
  Broadcast Variables are used in this example. Broadcasting is used when you want to map values in one file to another
  in a distributed manner
 */

object PopularMovieNames extends Serializable {

  def loadMovieNames(): Map[Int, String] = {

    //Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map Ints to Strings, and populate it from u.item
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("/Volumes/oli2/SparkDatasets/ml-100k/u.item")
    for (line <- lines.getLines()) {
      val fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()
    movieNames
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "PopularMovieNames")

    // Read in movie data
    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/ml-100k/u.data")

    // Create broadcast variable of ID: Int -> MovieName: String
    val nameDict = sc.broadcast(loadMovieNames())

    // Map data to (movieID, 1)
    val rdd = lines.map(line => (line.split("\t")(1).toInt, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2, ascending = false)

    // Map movie names to count
    val sortedMoviesWithNames = rdd.map(movie => (nameDict.value(movie._1), movie._2))

    // Collect and print results
    val results = sortedMoviesWithNames.collect()

    results.foreach(println)
  }
}
