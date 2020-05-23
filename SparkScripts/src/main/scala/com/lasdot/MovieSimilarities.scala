package com.lasdot

import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

object MovieSimilarities extends Serializable {

  //Type Definitions
  type MovieRating = (Int, Double) // (movieID, Rating)
  type UserRating = (Int, MovieRating) // (userID, MovieRating)
  type UserRatingPair = (Int, (MovieRating, MovieRating)) // (userID, (MovieRating, MovieRating))
  type RatingPair = (Double, Double) // (Rating 1, Rating 2)
  type RatingPairs = Iterable[RatingPair]

  /*
  * Compute Cosine Similarity of each movieID pair from an iterable RatingPairs
  * Return: (score, number of pairs)
  */
  def cosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val denominator: Double = Math.sqrt(sum_xx) * Math.sqrt(sum_yy)
    val numerator: Double = sum_xy

    val score: Double = numerator/denominator
    (score, numPairs)
  }

  // Return true for combos with ratings in a single direction - In this case movie 1 ID < movie 2 ID
  def filterDuplicates(line: UserRatingPair): Boolean = {
    line._2._1._1 < line._2._2._1
  }

  // Load Map of movie ID to movie names
  def loadMovies(): Map[Int, String] = {
    // Handle character encoding
    implicit val codec: Codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var idToName: Map[Int, String] = Map()

    val lines = Source.fromFile("/Volumes/oli2/SparkDatasets/ml-100k/u.item")

    for (line <- lines.getLines()) {
      val fields = line.split('|')
      if (fields.length > 1) {
        idToName += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()
    idToName
  }

  def makePairs(line: UserRatingPair): ((Int, Int), (Double, Double)) = {
    ((line._2._1._1, line._2._2._1), (line._2._1._2, line._2._2._2))
  }

  // Map (userID: Int, (MovieID: Int, Rating: Double)) for each line
  def userMovieRating(line: String): UserRating = {
    val fields = line.split("\\s+")
    (fields(0).toInt, (fields(1).toInt, fields(2).toDouble))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MovieSimilarity")

    val nameDict = loadMovies()

    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/ml-100k/u.data")

    val userMovieRatingRDD = lines.map(userMovieRating)

    /*
      Emit every movie rate together by the same user.
      TODO: Self-join to find every combination (Duplicates included)
     */
    val joinedRating = userMovieRatingRDD join userMovieRatingRDD

    //Filter out duplicate content
    val filteredRating = joinedRating.filter(filterDuplicates)

    // Make our (movieID 1, movieID 2) the keys and (Rating 1, Rating 2) the values
    val movieIDKeyPair = filteredRating.map(makePairs)

    /*
      We now have (movieID 1, movieID 2) => (Rating 1, Rating 2)
      Now we can collect all the ratings for each movie pair and compute the similarity
     */
    val moviePairRatings = movieIDKeyPair.groupByKey()

    /*
      Now that we have (movieID 1, movieID 2) => (Rating 1, Rating 2), (Rating 3, Rating 4), ...
      We can now compute the similarity
      TODO: (Comment) this is cached to avoid recomputing the rdd
     */
    val moviePairSimilarities = moviePairRatings.mapValues(cosineSimilarity).cache()

    //Extract similarities for movies high thresholds
    if (args.length > 0) {
      val scoreThreshold = 0.97
      val coOccurrenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      /*
      Filter for movies with similar to movieID passed in arguments with above thresholds
     */
      val filteredResults = moviePairSimilarities.filter(x => {
        val pair = x._1
        val sim = x._2
        (pair._1 == movieID || pair._2 == movieID) && (sim._1 > scoreThreshold) && (sim._2 > coOccurrenceThreshold)
      })

      //Sort by quality score
      val results = filteredResults.map(x => (x._2, x._1)).sortByKey(ascending = false).take(10)

      println("\nTo 10 similar movies for " + nameDict(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2

        //Display the similarity result that isn't the movie we're looking for
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(nameDict(similarMovieID) + " score: " + sim._1 + " strength: " + sim._2)
      }
    }
  }
}
