package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearRegDF extends java.io.Serializable {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("LinearRegDF")
      .master("local[*]")
      .getOrCreate()

    /*
    Load up our page speed / amount spent data in the format required by MLLib
    (label, feature vector)
    */
    val inputLines = spark.sparkContext.textFile("/Volumes/oli2/SparkDatasets/SparkScala/regression.txt")
    val data = inputLines.map(_.split(",")).map(x => (x(0).toDouble, Vectors.dense(x(1).toDouble)))

    import spark.implicits._

    //Convert RDD to DataFrame
    val colNames = Seq("label", "features") //Alternative to case class
    val df = data.toDF(colNames: _*) // _* means a list/sequence is being passed

    //Split dataset into training and test
    val Array(training, test) = df.randomSplit(Array(0.7, 0.3))

    val linReg = new LinearRegression()
      .setRegParam(0.3) //Regularization parameter
      .setElasticNetParam(0.8)
      .setMaxIter(100)
      .setTol(1E-6) // convergence tolerance

    val model = linReg.fit(training)

    /*
      Generate predictions using out model for all features in out test DataFrame.
     */
    val fullPredictions = model.transform(test).cache()

    //Extract the features and the "known" correct labels
    val resultAndLabel = fullPredictions.select("prediction", "label")
      .rdd
      .map(x => (x.getDouble(0), x.getDouble(1)))

    val rmseSquare = resultAndLabel.map(x => Math.pow(x._1 - x._2, 2.0)).sum()
    val rmse = Math sqrt rmseSquare/resultAndLabel.count()
    println("RMSE: " + rmse)

    for (prediction <- resultAndLabel) {
      println(prediction)
    }
    spark.stop()
  }
}
