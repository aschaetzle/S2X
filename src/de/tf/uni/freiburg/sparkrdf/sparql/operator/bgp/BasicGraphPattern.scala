package de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.broadcast.Broadcast
import scala.collection.JavaConversions._
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern
import de.tf.uni.freiburg.sparkrdf.model.graph.node._
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple._
import de.tf.uni.freiburg.sparkrdf.sparql.message._
import de.tf.uni.freiburg.sparkrdf.model.rdf.result._
import de.tf.uni.freiburg.sparkrdf.model.graph.edge.EdgeAttribute
import de.tf.uni.freiburg.sparkrdf.constants.Const
import org.apache.spark.storage.StorageLevel
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade
import org.apache.log4j.Logger
import org.apache.log4j.Priority
import org.apache.log4j.Level

/**
 * This trait implements the matching of the basic graph pattern. The matching is
 * divided into three big steps, where the last one can be repeated multiple times
 * until the result is correct.
 *
 * To start the matching, call matchBGP(triples)
 *
 * @author Thorsten Berberich
 */
trait BasicGraphPattern {

  protected def matchBGP(triples: Broadcast[java.util.List[TriplePattern]], graph: Graph[VertexInterface, EdgeAttribute], context: SparkContext): RDD[(VertexId, VertexInterface)] = {
    /*
     * Step 1:
     * 
     * Match all triple pattern with the graph triplets. This will be done with graph.triplets.flatMap().
     * The result of this step will be a set of messages which only contains AdjacentNodeMessages and 
     * NodeResultMessages. The first are used to store the parents and child nodes for every node
     * and the latter are used to store the mapping for the nodes.
     * 
     */
    var g: Graph[VertexInterface, EdgeAttribute] = graph
    var prevG: Graph[VertexInterface, EdgeAttribute] = null

    // Match all triple patterns and generate the messages for the nodes
    var messages = TriplePatternProcessor.matchBGP(triples, g)
    messages.persist(Const.STORAGE_LEVEL);

    // Join the results into the graph
    g = TriplePatternProcessor.joinResultsIntoGraph(messages, g)

    // Join back the neighbors into the nodes
    g = TriplePatternProcessor.joinNeighborsIntoGraph(messages, g)

    messages.unpersist(true);

    /*
     * Step 2:
     * 
     * Exchange the messages from the nodes to their parent and child nodes. This will be done 
     * with graph.vertices.flatMap(). These messages will be reduced down to one message per node and 
     * after that joined back into the nodes of the graph. The nodes which get messages from child or
     * parent nodes are then used in the next step to start the first iteration.
     */
    var newVerts: RDD[(VertexId, VertexInterface)] = null

    val adjacentMessages = AdjacentResultsExchanger.calculateResultsMessages(g.vertices, triples)
    newVerts = AdjacentResultsExchanger.joinMessagesIntoNodes(g, adjacentMessages).persist(StorageLevel.MEMORY_AND_DISK)

    prevG = g

    // Join the nodes back into the graph
    g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
    g.persist(Const.STORAGE_LEVEL)

    prevG.vertices.unpersist(true)
    prevG.edges.unpersist(true)

    /*
     * Step 3 + X:
     * 
     * In the following steps are the result of the nodes verified. This starts with the vertices 
     * from the previous step. The results of the nodes are then verified. If something changes, 
     * then the new results are sent to the parent and child nodes. The new vertices to process 
     * in the next round are the vertices which receive at least one message in the previous round.
     * 
     * Round i:	Input: 	vertices from round i - 1
     * 		Output:	Vertices which receive a message from the input vertices
     *   		input for round i + 1
     */
    var activeVertices: Boolean = false
    if (newVerts != null) {
      activeVertices = true
    }

    var oldVerts: RDD[(VertexId, VertexInterface)] = null;

    // Loop until no vertices are active
    while (activeVertices) {
      // Store RDDs that will be unpersisted after this round
      prevG = g
      oldVerts = newVerts

      // Verify the results of all nodes
      val verifiedNodesMsgs = NodeResultsVerifier.calculateResultsMessages(newVerts, triples)
      verifiedNodesMsgs.persist(StorageLevel.MEMORY_AND_DISK)

      // Vertices for the next round
      newVerts = NodeResultsVerifier.joinVerifiedResultsIntoNodes(verifiedNodesMsgs, g).persist(StorageLevel.MEMORY_AND_DISK)

      // Count the active vertices
      //      activeVertices = newVerts.count

      activeVertices = newVerts.mapPartitions(iter => Iterator(iter.hasNext), true).reduce(_ && _)

      // Join the nodes into the graph
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      g.vertices.persist(Const.STORAGE_LEVEL)

      // Unpersist RDDs
      oldVerts.unpersist(true)
      prevG.vertices.unpersist(true)
      prevG.edges.unpersist(true)
      verifiedNodesMsgs.unpersist(true)
    }

    // Filter out matching vertices
    val matching = g.vertices.filter(node => node._2.isMatching()).persist(Const.STORAGE_LEVEL)

    g.vertices.unpersist(true)
    g.edges.unpersist(true)
    if (newVerts != null) {
      newVerts.unpersist(true)
    }

    /*
     * Coalesce the partitions that the result can be built. Otherwise you could have a small 
     * amount of data in many partitions which could be a problem if you are doing joins with 
     * other RDDs with many partitions
     */
    val partitions = Math.max((matching.partitions.size / 5), 108)

    if (partitions < matching.partitions.size) {
      matching.coalesce(partitions, false).persist(StorageLevel.MEMORY_AND_DISK)
    } else {
      matching
    }
  }
}