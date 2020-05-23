package com.lasdot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.collection.mutable.ListBuffer

object GraphX extends Serializable {
  /*
    Create vertices by mapping heroID -> hero name tuples (or None in the case of failure)
    */
  def parseNames(line: String): Option[(VertexId, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1) {
      val heroID: Long = fields(0).trim().toLong //VertexId: Long
      if (heroID < 6487) {
        return Some(heroID, fields(1))
      }
    }
    None //FlatMap ignores None values
  }

  def makeEdges(line: String): List[Edge[Int]] = {
    val fields = line.split("\\s+")
    val origin = fields(0).toLong
    var edges = new ListBuffer[Edge[Int]]()

    for (i <- 1 until fields.length) {
      edges += Edge(origin, fields(i).toLong, 0)
    }
    edges.toList
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "GraphX")

    //Construct vertices
    val names = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/Marvel-names.txt")
    val V = names.flatMap(parseNames)

    //Build Edges
    val lines = sc.textFile("/Volumes/oli2/SparkDatasets/SparkScala/Marvel-graph.txt")
    val E = lines.flatMap(makeEdges)

    //Build graph, and cache it
    val default = "Nobody"
    val graph = Graph(V, E, default).cache()

    //Find the top 15 most connected superheros using graph.degrees
    println("\nTop 15 most featured superheros: ")
    //TODO: (Comment) degrees returns rdd of the count of individual connections to every vertex
    graph.degrees.join(V).sortBy(x => x._2._1, ascending = false).take(10).foreach(println)

    // Do BFS using Pregel API

    val root: VertexId = 6306 //Wolverine

    //Initialize all nodes with infinity except starting node
    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

    val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)(
      /*
        Our "vertex program" preserves the shortest distance
        between an inbound message and its current value.
        It receives the vertex ID we are operating on,
        the attribute already stored with the vertex, and
        the inbound message from this iteration.
       */
      (id, attr, msg) => Math.min(attr, msg),

      /*
        Our "send message" function propagates out to all neighbors
        with the distance incremented by one.
       */
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr+1))
        } else {
          Iterator.empty
        }
      },

      /*
        The "reduce" operation preserves the minimum
        of messages received by a vertex if multiple
        messages are received by one vertex
       */
      (a, b) => Math.min(a, b)
    ).cache()

    //Print out the first 100 results:
    println("\nFirst 100 results: for degrees from Wolverine")
    bfs.vertices.join(V).take(100).foreach(println)

    //Recreate our "degrees of separation" result:
    println("\n\nDegrees from Wolverine to Mephisto 3624")
    bfs.vertices.filter(x => x._1 == 3624).collect().foreach(println)
  }
}
