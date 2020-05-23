package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MostPopularSuperhero extends Serializable {

  def parseNames(line: String): Option[(Int, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1).split("/")(0))
    }
    None // NOTE: flatmap will discard None results, and extract data from Some results
  }

  def parseGraph(line: String): (Int, Int) = {
    val fields = line.split("\\s+")
    (fields(0).toInt, fields.length - 1)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MostPopularHero")

    //Load in names
    val names = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/Marvel-names.txt")
    val namesRDD = names.flatMap(parseNames)

    // Load in graph and create the mapping (heroID, (connections))
    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/Marvel-graph.txt")
    val rdd = lines.map(parseGraph)
      .reduceByKey((x, y) => x + y)

    // Flip rdd to (no_connections, heroID)
    val connToHeroID = rdd.map(x => (x._2, x._1))

    // Use max to find out the maximum key
    val mostPopularHeroID = connToHeroID.max()

    val mostPopularHero = namesRDD.lookup(mostPopularHeroID._2).head

    println("Most featured superhero is: " + mostPopularHero)
  }
}

