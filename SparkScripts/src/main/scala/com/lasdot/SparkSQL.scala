package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSQL extends Serializable {

  /*
   Case Class: Comes with accessors and constructors already builtin
   */
  case class Person(id: Int, name: String, age: Int, numFriends: Int)

  def mapper(line: String): Person = {
    val fields = line.split(",")
    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    person
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    /*
      Create Spark Session. In windows add config("spark.sql.warehouse.dir", "file:///C:/temp")
      TODO: (Comment) SparkSession returns a DataSet that SQL queries can be performed on
     */
    val sess = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    //TODO: (Comment) if you wanted to read json -- sess.read.json([filePath])
    val lines = sess.sparkContext.textFile("/Volumes/oli2/SparkDatasets/SparkScala/fakefriends.csv")
    val people = lines.map(mapper)

    /*
      Infer teh schema, and register the DataSet as a table.
      TODO (Comment) import sess.implicits._ must always be done before using .toDS()
     */
    import sess.implicits._
    val schemaPeople = people.toDS()

    schemaPeople.printSchema()

    //Create SQL representation
    schemaPeople.createOrReplaceTempView(viewName="people")

    //Run SQL query over DataFrame that has been registered as a table
    //TODO: (Comment) This SQL table is distributed amongst clusters
    val teenagers = sess.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

    val results = teenagers.collect()

    results.foreach(println)

    sess.stop()
  }
}
