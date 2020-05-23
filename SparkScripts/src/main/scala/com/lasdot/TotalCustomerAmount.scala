package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalCustomerAmount extends Serializable {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "MinCustomerAmount")

    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/customer-orders.csv")

    val rdd = lines
      .map(line => (line.split(",")(0), line.split(",")(2)
        .toDouble)).reduceByKey((x, y) => x + y).sortBy(_._2, ascending=true)

    val results = rdd.collect()
    results.foreach(println)
  }
}
