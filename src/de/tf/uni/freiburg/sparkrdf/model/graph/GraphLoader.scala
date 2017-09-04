package de.tf.uni.freiburg.sparkrdf.model.graph

import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import de.tf.uni.freiburg.sparkrdf.parser.rdf.RDFParser
import de.tf.uni.freiburg.sparkrdf.parser.rdf.LineMalformedException
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import de.tf.uni.freiburg.sparkrdf.model.graph.node._
import org.apache.spark.storage.StorageLevel
import de.tf.uni.freiburg.sparkrdf.model.graph.edge.EdgeAttribute
import org.apache.spark.broadcast.Broadcast
import de.tf.uni.freiburg.sparkrdf.constants.Const
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.hp.hpl.jena.shared.PrefixMapping

/**
 * Loader for the graph
 *
 * @author Thorsten Berberich
 *
 */
object GraphLoader extends Logging {
  /**
   * The original graph that was loaded
   */
  private var _originalGraph: Graph[VertexInterface, EdgeAttribute] = null;

  /**
   * Getter for the graph
   */
  def getGraph = {
    _originalGraph.vertices.setName("Original Graph Vertices")
    _originalGraph.edges.setName("Original Graph Edges")
    _originalGraph
  }

  /**
   * Constructs the initial graph if the graph is not loaded yet
   */
  def loadGraphCountBased(graphPath: String, context: SparkContext) {
    val conf: Configuration = new Configuration();
    val fs: FileSystem = FileSystem.get(conf);

    // Load the whole file into a new RDD
    val lines: RDD[String] = context.textFile(graphPath)

    // Split the line with the RDFParser, ignore prefixes
    val splitted: RDD[(String, String, String)] = lines.filter(line => line.length > 0 && !line.startsWith("@prefix") && !line.startsWith("@PREFIX"))
      // split the line
      .map(line => splitRDF(line, new RDFParser())).
      // Filter out line that were not well formed
      filter(line => line != null);

    splitted.persist(Const.STORAGE_LEVEL)

    // Get all subjects and all objects
    val subjects: RDD[String] = splitted.map(triple => triple._1)
    val objects: RDD[String] = splitted.map(triple => triple._3)

    // Get all nodes that are unique
    val distinctNodes: RDD[String] = context.union(subjects, objects).distinct;

    // Give every node a unique ID, which are needed for the graph
    val zippedNodes: RDD[(String, Long)] = distinctNodes.zipWithUniqueId
    zippedNodes.persist(Const.STORAGE_LEVEL)

    // Get the triples splitted
    val splittedTriples: RDD[(String, (String, String))] = splitted.map(triple =>
      // Object, (Subject, predicate)
      (triple._3, (triple._1, triple._2)))
    splitted.unpersist(true)

    // Join with the zipped nodes, to get the ID for the objects
    val joinedTriples = splittedTriples.join(zippedNodes)

    // map the result of the join to Subject, Array[ObjectID, Predicate]
    val replacedObjects: RDD[(String, (Long, String))] = joinedTriples.map(line => (line._2._1._1, (line._2._2, line._2._1._2)));

    // Replace the subjects by the unique node ID 
    val joinedSubjects: RDD[(String, ((Long, String), Long))] = replacedObjects.join(zippedNodes)

    var edges: RDD[Edge[EdgeAttribute]] = null

    if (Const.varPred) {
      // Map to predicate, subject Id, object Id, to join
      val replacedSubjects: RDD[(String, (Long, Long))] = joinedSubjects.map(line => (line._2._1._2, (line._2._2, line._2._1._1)));

      // Join the nodes for the predicate
      val predicateNodes: RDD[((Long, Long, String), Option[Long])] = replacedSubjects.leftOuterJoin(zippedNodes)
        // Map to (Subject Id, Object id, predicate), predicate id
        .map((pred => ((pred._2._1._1, pred._2._1._2, pred._1), pred._2._2)));

      // Build the edges out of the replaced nodes, startID, targetID,  String attribute = predicate
      edges = predicateNodes.map(line => {
        var attr: EdgeAttribute = null;

        // Check if there is an Id for a predicate
        line._2 match {
          case Some(predId) => {
            // Create an edge attribute with an Id
            attr = new EdgeAttribute(predId, line._1._3)
          }
          case None => {
            // Edge attribute without Id
            attr = new EdgeAttribute(line._1._3)
          }
        }
        Edge(line._1._1, line._1._2, attr)
      })
    } else {
      edges = joinedSubjects.map(line => Edge(line._2._2, line._2._1._1, new EdgeAttribute(line._2._1._2)))
    }

    // Build the nodes for the graph out of the node with unique IDs
    val nodes: RDD[(VertexId, VertexInterface)] = zippedNodes.map(node => {
      val id = node._2
      val attribute = node._1
      (id, new VertexAttribute(attribute))
    })
    zippedNodes.unpersist(true)

    _originalGraph = Graph(nodes, edges).persist(Const.STORAGE_LEVEL).partitionBy(PartitionStrategy.EdgePartition1D)

    // Save the result as object file
    if (Const.saveLoadOutput) {
      _originalGraph.vertices.saveAsObjectFile(fs.getHomeDirectory().toString() +
        "/" + Const.inputFile.replace("\\W", "_") + "/vertices")
      _originalGraph.edges.saveAsObjectFile(fs.getHomeDirectory().toString() +
        "/" + Const.inputFile.replace("\\W", "_") + "/edges")
    }
  }

  /**
   * Load the graph hash based
   */
  def loadGraphHashBased(graphPath: String, context: SparkContext) {
    val conf: Configuration = new Configuration();
    val fs: FileSystem = FileSystem.get(conf);

    // Load the whole file into a new RDD
    val lines: RDD[String] = context.textFile(graphPath)

    // Split the line with the RDFParser, ignore prefixes
    val splitted: RDD[(String, String, String)] = lines.filter(line => line.length > 0 && !line.startsWith("@prefix") && !line.startsWith("@PREFIX"))
      // split the line
      .map(line => splitRDF(line, new RDFParser())).
      // Filter out line that were not well formed
      filter(line => line != null);

    splitted.persist(Const.STORAGE_LEVEL)

    // Get all subjects and all objects
    val subjects: RDD[String] = splitted.map(triple => triple._1)
    val objects: RDD[String] = splitted.map(triple => triple._3)

    // Get all nodes that are unique
    val distinctNodes: RDD[String] = context.union(subjects, objects).distinct

    val hashedNodes: RDD[(VertexId, VertexInterface)] = distinctNodes.map(node => (hash(node), new VertexAttribute(node)))

    // Build the edges
    var edges: RDD[Edge[EdgeAttribute]] = null

    if (Const.varPred) {
      // Join the node ids to the predicates if a predicate also occurs als subject or object
      val predicatesToJoin: RDD[(String, (Long, Long))] = splitted.map(triple => (triple._2, (hash(triple._1), hash(triple._3))))
      val nodesToJoin: RDD[(String, VertexId)] = hashedNodes.map(node => (node._2.asInstanceOf[VertexAttribute].getAttribute(), node._1))

      val joinedPredicates = predicatesToJoin.leftOuterJoin(nodesToJoin)

      edges = joinedPredicates.map(line => {
        var attr: EdgeAttribute = null;

        // Check if there is an Id for a predicate
        line._2._2 match {
          case Some(predId) => {
            // Create an edge attribute with an Id
            attr = new EdgeAttribute(predId, line._1)
          }
          case None => {
            // Edge attribute without Id
            attr = new EdgeAttribute(line._1)
          }
        }
        Edge(line._2._1._1, line._2._1._2, attr)
      })
    } else {
      edges = splitted.map(triple => Edge(hash(triple._1), hash(triple._3), new EdgeAttribute(triple._2)))
    }

    splitted.unpersist(true)
    _originalGraph = Graph(hashedNodes, edges).persist(Const.STORAGE_LEVEL).partitionBy(PartitionStrategy.EdgePartition1D)

    // Save the result as object file
    if (Const.saveLoadOutput) {
      _originalGraph.vertices.saveAsObjectFile(fs.getHomeDirectory().toString() +
        "/" + Const.inputFile.replace("\\W", "_") + "/vertices")
      _originalGraph.edges.saveAsObjectFile(fs.getHomeDirectory().toString() +
        "/" + Const.inputFile.replace("\\W", "_") + "/edges")
    }

    // Check if there are hash collisions
    val hashes = hashedNodes.map(node => node._1)
    val hashedCount = hashes.count
    val distHashedCount = hashes.distinct.count

    if (hashedCount.compareTo(distHashedCount) != 0) {
      // Collisions existing
      this.logError("Collisions occured while hashing. The graph can not be loaded with hashing. Use the option -countBased to load the graph without hashing.")
    }
  }

  /**
   * Load a preloaded graph
   */
  def loadPreloadedGraph(graphPath: String, context: SparkContext) {
    val conf: Configuration = new Configuration();
    val fs: FileSystem = FileSystem.get(conf);
    // Load the preloaded object file
    val nodes: RDD[(VertexId, VertexInterface)] = context.objectFile(fs.getHomeDirectory().toString() +
      "/" + Const.inputFile.replace("\\W", "_") + "/vertices")
    val edges: RDD[Edge[EdgeAttribute]] = context.objectFile(fs.getHomeDirectory().toString() +
      "/" + Const.inputFile.replace("\\W", "_") + "/edges")

    _originalGraph = Graph(nodes, edges).persist(Const.STORAGE_LEVEL)
    _originalGraph
  }

  /**
   * Splits a String into an String array
   */
  def splitRDF(line: String, parser: RDFParser): (String, String, String) = {
    try {
      val parsed = parser.parse(line);
      return (parsed.apply(0), parsed.apply(1), parsed.apply(2));
    } catch {
      case e: LineMalformedException => return null
    }
  }

  /**
   * Used 64 bit hash function
   *
   * @see http://stackoverflow.com/questions/1660501/what-is-a-good-64bit-hash-function-in-java-for-textual-strings
   */
  def hash(string: String): Long = {
    var h = 1125899906842597L; // prime
    val len = string.length();

    for (i <- 0 to len - 1) {
      h = 31 * h + string.charAt(i);
    }
    h
  }
}