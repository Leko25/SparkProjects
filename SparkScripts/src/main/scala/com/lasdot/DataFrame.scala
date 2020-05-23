package com.lasdot

import com.lasdot.SparkSQL.mapper
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

//noinspection DuplicatedCode
object DataFrame extends Serializable {

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
    import sess.implicits._
    val lines = sess.sparkContext.textFile("/Volumes/oli2/SparkDatasets/SparkScala/fakefriends.csv")
    val people = lines.map(mapper).toDS().cache()

    people.printSchema()

    //Show the top 20 results for the name field
    println("Show the top 20 results for name field")
    people.select("name").show()

    //Filter by age
    println("Filtered DataFrame by age < 21")
    people.filter(people("age") < 21).show()

    println("Grouping by age")
    people.groupBy("age").count().show()

    println("Make everyone 10years older")
    people.select(people("name"), people("age") + 10).show()

    sess.stop()
  }
}
