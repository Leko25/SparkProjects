package com.lasdot

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable
import scala.io.{Codec, Source}

object MovieRecommendationsALS extends java.io.Serializable {
  case class Rating(userId: Int, movieId: Int, rating: Float)

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

  def parseRatings(line: String): Rating = {
    val fields = line.split("\t")
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sess = SparkSession
      .builder()
      .appName("MovieRecommendationALS")
      .master("local[*]")
      .getOrCreate()

    import sess.implicits._

    println("Loading movie names...")
    val nameDict = loadMovieNames()

    val data = sess.read.textFile("/Volumes/oli2/SparkDatasets/ml-100k/u_copy.data")

    val ratingsDF = data.map(parseRatings).toDF()
    ratingsDF.show()

    println("\nTraining recommendation model...")

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    // Fit Model to rating

    val model = als.fit(ratingsDF)

    // Get top 15 recommendations for specified user
    val userID: Int = args(0).toInt
    // Construct a DataFrame of a single user
    val users = Seq(userID).toDF("userId")
    val recommendations = model.recommendForUserSubset(users, 15)

    println("\nTop 15 recommendations for user ID" + userID + ":")

    for (recommendation <- recommendations) {
      val userRec = recommendation(1) // The first column is the userId, second is the recommendation
      // TODO: (Comment) userRec is a WrappedArray object and scala has to be told about it
      val temp = userRec.asInstanceOf[mutable.WrappedArray[Row]]
      for (rec <- temp) {
        val movie = rec.getAs[Int](0)
        val rating = rec.getAs[Float](1)
        val movieName = nameDict(movie)
        println(movieName, rating)
      }
    }
    sess.stop()
  }
}
