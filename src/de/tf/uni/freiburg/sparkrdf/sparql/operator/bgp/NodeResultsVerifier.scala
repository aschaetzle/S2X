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
 *
 * Validate the match set
 *
 * @author Thorsten Berberich
 */
object NodeResultsVerifier {

  /**
   * Verify the results of all nodes and then send the new results to the parent and child nodes.
   */
  def calculateResultsMessages(vertices: RDD[(VertexId, VertexInterface)],
    triples: Broadcast[java.util.List[TriplePattern]]): RDD[(VertexId, IMessage)] = {
    // Generate the message for each node
    val messages = vertices.flatMap(idNode => {
      val node: VertexAttribute = idNode._2.asInstanceOf[VertexAttribute];

      /*
       * Verify the results of the node. If the method returned true then 
       * something was changed in the result. This means that the results
       *  have to be sent to the adjacent nodes.
       */
      if (node.verifyNodeMatches(triples.value, false)) {
        // All results
        val result = scala.collection.mutable.ArrayBuffer.empty[(Long, IMessage)]

        // Check if there are parent nodes
        if (node.hasParentNodes()) {
          // Add all messages for parents
          node.getParentIDs().map(Long2long).foreach(parent => {

            var sendToParents: IMessage = null;

            // If there are any results, send them to the parents
            if (node.hasResults()) {
              sendToParents = new ResultMessage();
              sendToParents.addChildResults(node.getSendResults(parent, triples.value))
            } else {
              /* 
             * There are no result. Send a message that the parent node
             * can remove the results of this node and from the child list
             */
              sendToParents = new RemoveResultsMessage(node.getAttribute())
            }
            result += ((parent, sendToParents))
          })
        }

        // Check if the node has children
        if (node.hasChildNodes()) {
          // Add all messages for parents
          node.getChildIDs().map(Long2long).foreach(child => {

            var sendToChildren: IMessage = null

            // If there are any results, send them to the children
            if (node.hasResults()) {
              sendToChildren = new ResultMessage();
              sendToChildren.addParentResults(node.getSendResults(child, triples.value))
            } else {
              /* 
             * There are no result. Send a message that the child node
             * can remove the results of this node and from the parent list
             */
              sendToChildren = new RemoveResultsMessage(node.getAttribute())
            }
            result += ((child, sendToChildren))
          })
        }

        // Add a message for the node with the updated results
        val id: Long = idNode._1
        result += ((id, new UpdateMessage(node.getResults())))

        // Return the result
        result
      } else {
        // Nothing has changed, no need so send messages
        Iterator.empty
      }
    });

    messages
  }

  /**
   * Joins the updated results of neighboring nodes in to the nodes. The messages could
   * also contain RemoveResultMessages. The steps are:
   *
   * 1. Reduce the ResultMessages and RemoveResultMessages down to one for each node
   * 2. Join the results into the nodes
   *
   *  @param Graph Graph to join back the results into the nodes
   *  @param messages Messages which contain NodeResultMessages and RemoveResultMessages
   *
   *  @return Nodes with updated neighbor results
   *
   */
  def joinVerifiedResultsIntoNodes(messages: RDD[(VertexId, IMessage)],
    graph: Graph[VertexInterface, EdgeAttribute]): RDD[(VertexId, VertexInterface)] = {

    // Reduce all messages for a node down to one
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
        node.updateChildMatches(message.getChildResults())
      }

      // Update the parent results if there are any
      if (message.hasParentResults()) {
        node.updateParentMatches(message.getParentResults())
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