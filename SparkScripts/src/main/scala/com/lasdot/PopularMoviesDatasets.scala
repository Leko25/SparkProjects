package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import PopularMovieNames.loadMovieNames
import org.apache.spark.sql.functions._

object PopularMoviesDatasets extends Serializable {
  final case class Movie(id: Int)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sess = SparkSession
      .builder()
      .appName("PopularMoviesDatasets")
      .master("local[*]")
      .getOrCreate()

    val lines = sess.sparkContext.textFile("/Volumes/oli2/SparkDatasets/ml-100k/u.data").map(x => Movie(x.split("\\s+")(1).toInt))

    //Covert to spark Dataset
    import sess.implicits._
    val movieDS = lines.toDS()

    //Sort all movies by popularity into one line
    val topMovies = movieDS.groupBy(movieDS("id")).count().orderBy(desc("count")).cache()

    topMovies.show()

    val namesDict = loadMovieNames()

    for (result <- topMovies) {
      println (namesDict(result(0).asInstanceOf[Int]) + ": " + result(1))
    }
    sess.stop()
  }
}
