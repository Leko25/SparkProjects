package com.lasdot

import java.io.{FileNotFoundException, IOException}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MinTempFilter extends Serializable {

  def parseLine(line: String): (String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)

    //Convert temperature from Celsius to Fahrenheit
    val temperature = fields(3).toFloat * 0.1f * (9.0f/5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  def print(results: Array[(String, Float)]): Unit = {
    for (result <- results) {
      println("StationID: " + result._1 + " | MinTemperature: " + result._2)
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinTemp")

    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/1800.csv")

    val rdd = lines.map(parseLine)
    rdd.take(4).foreach(println)
    println("Size before filter: " + rdd.count())

    //Only use lines that have "TMIN" field
    val minTemp = rdd.filter(x => x._2.contentEquals("TMIN"))
    println("Size after filter: " +  minTemp.count())

    // Get minimum temperature for each sectionID
    val minTempByStation = minTemp.map(x => (x._1, x._3)).reduceByKey((x, y) => Math.min(x, y))

    val result = minTempByStation.collect().sortBy(x => x._2)
    print(result)
  }
}
