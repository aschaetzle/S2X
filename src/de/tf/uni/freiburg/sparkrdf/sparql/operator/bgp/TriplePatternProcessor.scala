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
import de.tf.uni.freiburg.sparkrdf.constants.Const
import java.util.ArrayList

/**
 * This Scala object is used to generate the messages which are used to store the result for the nodes.
 * The messages are reduced and stored in the node These nodes are then stored in the graph.
 *
 * @author Thorsten Berberich
 */
object TriplePatternProcessor {

  /**
   * Match all TriplePattern against all triplets of the graph
   */
  def matchBGP(triples: Broadcast[java.util.List[TriplePattern]],
    graph: Graph[VertexInterface, EdgeAttribute]): RDD[(VertexId, IMessage)] = {

    graph.triplets.flatMap(triplet => {
      // Result iterator
      val result = scala.collection.mutable.ArrayBuffer.empty[(Long, IMessage)]

      val subjectNode: VertexAttribute = triplet.srcAttr.asInstanceOf[VertexAttribute];
      val objectNode: VertexAttribute = triplet.dstAttr.asInstanceOf[VertexAttribute];
      val edge: EdgeAttribute = triplet.attr;

      // Match TriplePattern to this triplet
      triples.value.foreach(queryTriple => {

        // Check if the TriplePattern is fulfilled
        if (queryTriple.isFulfilledByTriplet(subjectNode, edge.getEdgeAttribute(), objectNode)) {

          /* 
           * Add the result for the subject node to the result iterator.
           * It looks like this:
           * ((TriplePattern.getStringRepresentation(), SUBJECT), edgeAttribute, objectNodeAttribute)
           */
          val subResultValue: ResultValue = new ResultValue(edge.getEdgeAttribute(), objectNode.getAttribute());
          val subkey: CompositeKey = new CompositeKey(queryTriple.getStringRepresentation(), Position.SUBJECT)
          val subjectResult: NodeResultMessage = new NodeResultMessage(subkey, subResultValue);

          // Add the subject result to the iterator
          result += ((triplet.srcId, subjectResult))

          /* 
           * Add the result for the object node to the result iterator.
           * It looks like this:
           * ((TriplePattern.getStringRepresentation(), OBJECT), edgeAttribute, subjectNodeAttribute)
           */

          if (!TriplePatternUtils.isObjectLiteral(triples.value, queryTriple.getObject())) {
            val objResultValue: ResultValue = new ResultValue(edge.getEdgeAttribute(), subjectNode.getAttribute());
            val objKey: CompositeKey = new CompositeKey(queryTriple.getStringRepresentation(), Position.OBJECT)
            val objectResult: NodeResultMessage = new NodeResultMessage(objKey, objResultValue);

            // Add the object result to the iterator
            result += ((triplet.dstId, objectResult))

            // Send the message to the child node with the parent id and the other way round
            val toChild: AdjacentNodeMessage = new AdjacentNodeMessage(new AdjacentNode(triplet.srcId, subjectNode.getAttribute(), true))
            val toParent: AdjacentNodeMessage = new AdjacentNodeMessage(new AdjacentNode(triplet.dstId, objectNode.getAttribute(), false))
            result += ((triplet.dstId, toChild))
            result += ((triplet.srcId, toParent))
          }

          /* 
           * Check if the predicate is a variable field and the edge has a node id.
           * This means, that the variable predicate field also occurs somewhere as node in the graph.
           */
          if (queryTriple.isPredicateVariable() && edge.hasNodeId()) {

            /*
             * Add the result for the predicate node
             * It looks like this:
             * ((TriplePattern.getStringRepresentation(), PREDICATE), subjectNodeAttribute, objectNodeAttribute)
             */
            val predResultValue: ResultValue = new ResultValue(subjectNode.getAttribute(), objectNode.getAttribute());
            val predKey: CompositeKey = new CompositeKey(queryTriple.getStringRepresentation(), Position.PREDICATE)
            val predicateResult: NodeResultMessage = new NodeResultMessage(predKey, predResultValue);

            // Add the predicate result to the iterator
            val id: Long = triplet.attr.getNodeId().asInstanceOf[scala.Long]
            result += ((id, predicateResult))

            /*
             * The subject node is a parent node of the predicate node
             * and the object node is the child of the predicate node 
             */
            val toChild2 = new AdjacentNodeMessage(new AdjacentNode(triplet.srcId, subjectNode.getAttribute(), true))
            val toParent2 = new AdjacentNodeMessage(new AdjacentNode(triplet.dstId, objectNode.getAttribute(), false))
            result += ((id, toChild2))
            result += ((id, toParent2))

            /*
             * The predicate node is the child of the subject node and
             * the parent of the object node
             */
            val toChild = new AdjacentNodeMessage(new AdjacentNode(id, edge.getEdgeAttribute(), true))
            val toParent = new AdjacentNodeMessage(new AdjacentNode(id, edge.getEdgeAttribute(), false))
            result += ((triplet.dstId, toChild))
            result += ((triplet.srcId, toParent))
          }
        }

      })

      // If the result iterator is empty use an empty Iterator
      if (result.size > 0) {
        result.iterator
      } else {
        Iterator.empty
      }
    })
  }

  /**
   * Joins the results back into the graph. The steps are:
   * 1. Filter out the NodeResultMessages out of the given messages
   * 2. Reduce the NodeResultMessages down to one for each node
   * 3. Join the results into the nodes
   * 4. Join the nodes into the graph
   *
   *  @param msgs Messages which contain NodeResultMessages, could also contain other IMessage-Typed objects
   *  @param graph Graph to join back the results into the nodes
   *
   *  @return Graph with updated nodes
   *
   */
  def joinResultsIntoGraph(msgs: RDD[(VertexId, IMessage)], graph: Graph[VertexInterface, EdgeAttribute]): Graph[VertexInterface, EdgeAttribute] = {
    // Filter out the messages that contain results for a node
    val filtered: RDD[(VertexId, IMessage)] = msgs.filter(msg => {
      msg._2 match {
        case matches: NodeResultMessage => true;
        case other => false;
      }
    })

    val distincted = filtered.distinct

    // Reduce all messages for one node down to one
    val results = distincted.reduceByKey((first: IMessage, second: IMessage) => {
      val nodeRes: NodeResultMessage = first.asInstanceOf[NodeResultMessage];
      nodeRes.addResults(second.getResults())
      nodeRes
    })

    // Join the message content into the nodes
    val newVerts: RDD[(VertexId, VertexInterface)] = graph.vertices.innerJoin(results)((vertexId, node, message) => {
      /* 
       * Make a deep copy of the node, that the nodes of the original graph keep unchanged.
       * This is necessary for the upcoming BGP matching processes
       */
      val newNode = node.deepCopy()
      newNode.addResults(message.getResults())
      newNode
    })

    // Join the nodes back into the graph
    val newGraph = graph.outerJoinVertices(newVerts) {
      (vid, old, newOpt) => newOpt.getOrElse(old)
    }
    newGraph
  }

  /**
   * Joins the parent and child id back into nodes. The steps are:
   * 1. Filter out the AdjacentNodesMessages out of the given messages
   * 2. Reduce the AdjacentNodesMessages down to one for each node
   * 3. Join the adjacent node ids into the nodes
   * 4. Join the nodes into the graph
   *
   *  @param msgs Messages which contain AdjacentNodesMessages, could also contain other IMessage-Typed objects
   *  @param graph Graph to join back the adjacent node information into the nodes
   *
   *  @return Graph with updated nodes
   *
   */
  def joinNeighborsIntoGraph(msgs: RDD[(VertexId, IMessage)], graph: Graph[VertexInterface, EdgeAttribute]): Graph[VertexInterface, EdgeAttribute] = {
    // Filter out the messages that contain adjacent node information for a node
    val filtered = msgs.filter(msg => {
      msg._2 match {
        case adjMsg: AdjacentNodeMessage => true;
        case other => false;
      }
    })

    // Reduce the messages down to one for every node
    val adjacentNodes = filtered.reduceByKey((first: IMessage, second: IMessage) => {
      val firstMsg = first.asInstanceOf[AdjacentNodeMessage]
      val scndMsg = second.asInstanceOf[AdjacentNodeMessage]

      firstMsg.addAdjacentNodes(scndMsg.getAdjacentNodes())
      firstMsg
    })

    // Join the information in to the nodes of the graph
    val newVerts = graph.vertices.innerJoin(adjacentNodes)((vertexId, node, message) => {
      val msg = message.asInstanceOf[AdjacentNodeMessage];

      msg.getAdjacentNodes().foreach(adjNode => {

        if (adjNode.isParent()) {
          node.addParentNode(adjNode.getId(), adjNode.getAttribute())
        } else {
          node.addChildNode(adjNode.getId(), adjNode.getAttribute())
        }

      })
      node
    })

    val prevG = graph

    // Join the nodes back into the graph
    val newGraph = graph.outerJoinVertices(newVerts) {
      (vid, old, newOpt) => newOpt.getOrElse(old)
    }

    prevG.vertices.unpersist(true)
    prevG.edges.unpersist(true)

    newGraph
  }
}