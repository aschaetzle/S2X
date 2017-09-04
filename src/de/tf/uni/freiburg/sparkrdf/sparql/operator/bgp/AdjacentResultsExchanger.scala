package de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp

import org.apache.spark.rdd._
import org.apache.spark._
import de.tf.uni.freiburg.sparkrdf.sparql.message._
import org.apache.spark.graphx._
import org.apache.spark.broadcast.Broadcast
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import de.tf.uni.freiburg.sparkrdf.model.graph.edge.EdgeAttribute
import de.tf.uni.freiburg.sparkrdf.model.graph.node._
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple._
import de.tf.uni.freiburg.sparkrdf.sparql.message._
import de.tf.uni.freiburg.sparkrdf.sparql.message.datatype._
import de.tf.uni.freiburg.sparkrdf.model.rdf.result._

/**
 * Scala object which is used to calculate the messages to the parent and child nodes, which contain the results
 * of the node and to join this messages back into the nodes of the graph.
 *
 * @author Thorsten Berberich
 *
 */
object AdjacentResultsExchanger {

  /**
   * Calculate the messages which are used to exchange the results from all nodes to their parent and child nodes.
   * The ResultMessage-Type is used to achieve this. For every node a message for all parents and all child nodes
   * are created if there are any.
   */
  def calculateResultsMessages(verts: RDD[(VertexId, VertexInterface)],
    triples: Broadcast[java.util.List[TriplePattern]]): RDD[(VertexId, IMessage)] = {
    // Create messages from all nodes
    verts.flatMap(idNode => {
      // The actual node
      val node: VertexAttribute = idNode._2.asInstanceOf[VertexAttribute];

      // List of result messages
      val result = scala.collection.mutable.ArrayBuffer.empty[(Long, IMessage)]

      // Only send messages if the node is matching some part of the BGP
      if (node.isMatching()) {
        node.verifyNodeMatches(triples.value, true)

        if (node.hasResults()) {
          // Check if the node has parent nodes
          if (node.hasParentNodes()) {

            // Add the message for all parents
            node.getParentIDs().map(Long2long).foreach(parent => {
              // Message with node results as child results
              val sendChildren = new ResultMessage();

              /* 
               * Get the results in the version that could be sent to other nodes. 
               * That means that the result contains the node attribute.
               */
              sendChildren.addChildResults(node.getSendResults(parent, triples.value))

              result += ((parent, sendChildren))
            })
          }

          // Check if the node has child nodes
          if (node.hasChildNodes()) {

            // Add the message for all children
            node.getChildIDs().map(Long2long).foreach(child => {

              // Create the message
              val sendParents = new ResultMessage();
              sendParents.addParentResults(node.getSendResults(child, triples.value))

              result += ((child, sendParents))
            })
          }
          result
        } else {
          if (node.hasParentNodes()) {
            val sendToParents = new RemoveResultsMessage(node.getAttribute())
            // Add the message for all parents
            node.getParentIDs().map(Long2long).foreach(parent => {
              result += ((parent, sendToParents))
            })
          }

          if (node.hasChildNodes()) {
            val sendToChildren = new RemoveResultsMessage(node.getAttribute())
            // Add the message for all parents
            node.getChildIDs().map(Long2long).foreach(parent => {
              result += ((parent, sendToChildren))
            })
          }
          result
        }

        // Add a message for the node with the updated results
        val id: Long = idNode._1
        result += ((id, new UpdateMessage(node.getResults())))

        result
      } else {
        // Empty result, because the node doesn't match a part of the BGP
        Iterator.empty
      }
    })
  }

  /**
   * Joins all results of the neighbors back into the nodes. The steps are:
   *
   * 1. Reduce the ResultMessages down to one for each node
   * 2. Join the results into the nodes
   *
   *  @param Graph Graph to join back the results into the nodes
   *  @param messages Messages which contain ResultMessages
   *
   *  @return Nodes with updated neighbor results
   *
   */
  def joinMessagesIntoNodes(graph: Graph[VertexInterface, EdgeAttribute], messages: RDD[(VertexId, IMessage)]): RDD[(VertexId, VertexInterface)] = {
    // Reduce the messages down to one for every node
    val reducedMessages = messages.reduceByKey((msg1, msg2) => {

      /*
       * Create a new ResultMessage-object, because we don't know which classes 
       * msg1 and msg2 are. It could also be the case that they are both 
       * RemoveResultMessages and they are not expandable (yet). So in 
       * that case they couldn't be reduced. To bypass this every reduce
       * operation is a new ResultMessage created
       */
      val result: ResultMessage = new ResultMessage();

      // Add all results from msg1
      if (msg1.hasChildResults()) {
        result.addChildResults(msg1.getChildResults())
      }
      if (msg1.hasParentResults()) {
        result.addParentResults(msg1.getParentResults())
      }
      if (msg1.hasRemoveAttributes()) {
        result.addRemoveIds(msg1.getRemoveAttributes())
      }

      // Add the result update for the node if it is not null
      if (msg1.getResults() != null) {
        result.addResults(msg1.getResults())
      }

      // Add all results from msg2
      if (msg2.hasChildResults()) {
        result.addChildResults(msg2.getChildResults())
      }
      if (msg2.hasParentResults()) {
        result.addParentResults(msg2.getParentResults())
      }
      if (msg2.hasRemoveAttributes()) {
        result.addRemoveIds(msg2.getRemoveAttributes())
      }

      // Add the result update for the node if it is not null
      if (msg2.getResults() != null) {
        result.addResults(msg2.getResults())
      }

      result
    })

    // Join the content of the messages back into the nodes of the graph
    val newVerts = graph.vertices.innerJoin(reducedMessages)((vertexId, node, message) => {
      // Update the child results if there are any
      if (message.hasChildResults()) {
        node.addChildMatches(message.getChildResults())
      }

      // Update the parent results if there are any
      if (message.hasParentResults()) {
        node.addParentMatches(message.getParentResults())
      }

      // Remove all child and parent results from nodes which don't match anything anymore
      if (message.hasRemoveAttributes()) {
        message.getRemoveAttributes().foreach(attr => node.removeAdjacentMatches(attr))
      }

      // Update the node results
      node.updateNodeResults(message.getResults())
      node
    })

    newVerts
  }

}