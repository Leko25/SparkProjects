package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

/*
 BFS Implementation in Apache Spark using Marvel-names dataset
 */

object DegreesOfSeparationMarvel extends Serializable{
  // Characters we wish to find separation between
  val startCharacterID = 5306 // SpiderMan
  val targetCharacterID = 3624 // Mephisto

  //We can make our accumulator a "global" option so we can reference it in a mapper later
  var hitCounter:Option[LongAccumulator] = None

  // Custom DataTypes
  // BFSData contains an array of (heroID connections: Array[INT], DISTANCE: INT, and COLOR: STRING).
  type BFSData = (Array[Int], Int, String)

  // A BFSNode has (heroID connections, BFSData)
  type BFSNode = (Int, BFSData)

  /*
    Expands a BFSNode into its Node and its Children
    Steps:
    -----
    1) Create an Empty Array of BFSNodes
    2) Take a GRAY BFSNode (heroID: Int, (connections: Array[Int], DISTANCE: Int, COLOR: String))
    3) Loop through its connections and create new GRAY BFSNodes
    4) Add this new GRAY BFSNodes to the results array
    5) If the Node is the target Node then increment the accumulator
    6) Color the explored node BLACK and add it back into the results
   */
  def bfsMap(node: BFSNode): Array[BFSNode] = {
    // Extract data from the BFSNode
    val characterID: Int = node._1
    val data: BFSData = node._2

    val connections: Array[Int] = data._1
    val DISTANCE: Int = data._2
    var COLOR: String = data._3

    /*
      This is called from flatMap, so we return an array of potentially many
      BFSNodes to add to our new RDD
     */
    var results: ArrayBuffer[BFSNode] = new ArrayBuffer[BFSNode]()

    /*
      GRAY colored nodes are flagged for expansion, and we create new
      GRAY nodes for each connection. Note: In BFS
     */
    if (COLOR == "GRAY") {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDISTANCE = DISTANCE + 1
        val newCOLOR = "GRAY"

        /*
          IF node is the target node then increment the accumulator to halt the driver program
         */
        if (targetCharacterID == newCharacterID) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }

        // Create a new Grey node for this connection and add it to the results
        val newGRAYNode: BFSNode = (newCharacterID, (Array(), newDISTANCE, newCOLOR))
        results += newGRAYNode
      }

      // COLOR is explored node as BLACK
      COLOR = "BLACK"
    }

    /*
      Add the explored node back in, so that its connections can get merged with
      the GRAY nodes in the reducer
     */
    val thisGRAYNode: BFSNode = (characterID, (connections, DISTANCE, COLOR))
    results += thisGRAYNode
    results.toArray
  }

  def bfsReduce(data1: BFSData, data2: BFSData): BFSData = {

    //Extract data to combine
    val connections1: Array[Int] = data1._1
    val connections2: Array[Int] = data2._1
    val DISTANCE1: Int = data1._2
    val DISTANCE2: Int = data2._2
    val COLOR1: String = data1._3
    val COLOR2: String = data2._3

    // Default node values
    var DISTANCE: Int = Integer.MAX_VALUE
    var COLOR: String = "WHITE"
    var connections: ArrayBuffer[Int] = new ArrayBuffer[Int]()

    /*
      See if one is the original node with its connections. If so preserve them
     */
    if (connections1.length > 0) {
      connections ++= connections1
    }
    if (connections2.length > 0) {
      connections ++= connections2
    }

    // Preserve the minimum distance
    if (DISTANCE1 < DISTANCE) {
      DISTANCE = DISTANCE1
    }

    if (DISTANCE2 < DISTANCE2) {
      DISTANCE = DISTANCE2
    }

    //Preserve the darkest COLOR
    if (COLOR1.contentEquals("WHITE") && (COLOR2.contentEquals( "GRAY") || COLOR2.contentEquals("BLACK"))) {
      COLOR = COLOR2
    } else if (COLOR1.contentEquals("GRAY") && COLOR2.contentEquals("BLACK")) {
      COLOR = COLOR2
    } else if (COLOR2.contentEquals("WHITE") && (COLOR1.contentEquals( "GRAY") || COLOR1.contentEquals("BLACK"))) {
      COLOR = COLOR1
    } else {
      COLOR = COLOR1
    }
    (connections.toArray, DISTANCE, COLOR)
  }

  /*
    Converts each line into a BFSNode
   */
  def convertToBFSData(line: String): BFSNode = {

    // Split line based on white-space
    val fields = line.split("\\s+")
    val heroID = fields(0).toInt
    var COLOR: String = "WHITE"
    var DISTANCE: Int = Integer.MAX_VALUE

    val connections: ArrayBuffer[Int] = new ArrayBuffer[Int]()

    for (connection <- 1 until fields.length) {
      connections += fields(connection).toInt
    }

    if (heroID == startCharacterID) {
      COLOR = "GRAY"
      DISTANCE = 0
    }
    (heroID, (connections.toArray, DISTANCE, COLOR))
  }

  def initRDD(sc: SparkContext): RDD[BFSNode] = {
    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/Marvel-graph.txt")
    lines.map(convertToBFSData)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "DOSMarvel")

    // Our Accumulator is used to signal when we find the target character in our BFS traversal
    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    /*
      Read in file and create rdd from createBFSData function (heroID, (connections: Array[Int], DISTANCE: Int, COLOR: String))
     */
    var iterationRDD = initRDD(sc)

    for (iter <- 1 to 10) {
      println("Running BFS iteration# " + iter)

      /*
        Create new vertices as needed to darken or reduce distance in the reduce stage.
        If we encounter the target node as GRAY node - we increment our accumulator to signal that we are done
       */
      val mapped = iterationRDD.flatMap(bfsMap)

      /*
        TODO: (COMMENT) mapped.count() action forces the RDD to be reevaluated, and this is the only that our accumulator Gets updated
       */
      println("Processing: " + mapped.count() + " values")
      if (hitCounter.isDefined) {
        val hitCount =  hitCounter.get.value
        if (hitCount > 0) {
          println("Target Character encountered! From " + hitCount + " different direction(s).")
          return
        }
      }

      /*
        Reducer combines data for each characterID, preserving the darkest color and shortest path
       */
      iterationRDD = mapped.reduceByKey(bfsReduce)
    }
  }
}
