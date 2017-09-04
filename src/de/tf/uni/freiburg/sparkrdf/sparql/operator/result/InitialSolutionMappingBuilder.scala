package de.tf.uni.freiburg.sparkrdf.sparql.operator.result

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.JavaConversions._
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern
import de.tf.uni.freiburg.sparkrdf.model.graph.node._
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple._
import de.tf.uni.freiburg.sparkrdf.sparql.message._
import de.tf.uni.freiburg.sparkrdf.model.rdf.result._
import de.tf.uni.freiburg.sparkrdf.model.graph.edge.EdgeAttribute
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping

/**
 * Object that is used to extract the results out of the given graph vertices
 *
 * @author Thorsten Berberich
 */
object InitialSolutionMappingBuilder {

  /**
   * Convert the vertex results from the graph into solution mappings that can be joined later on
   */
  def findVertexResultNodes(toSearch: java.util.Set[TriplePattern], vertices: RDD[(VertexId, VertexInterface)]): RDD[SolutionMapping] = {

    vertices.flatMap(vertex => {
      var result = List[SolutionMapping]()

      /* 
       * Create a solution mapping if the vertex is matching some part of the 
       * basic graph pattern
       */
      val results = vertex._2.getResults()

      // Search all results of the node
      results.foreach(resultPair => {
        val key: CompositeKey = resultPair._1

        // Take only results where the node is a subject
        if (key.getPosition().equals(Position.SUBJECT)) {

          // Check if it matches a triple pattern
          toSearch.foreach(triple => {

            if (key.getTriplePattern().equals(triple.getStringRepresentation())) {

              // Add all results of this node
              resultPair._2.foreach(resultValue => {

                // Add all variable fields to the solution mapping
                val solutionMapping: SolutionMapping = new SolutionMapping(key.getTriplePattern())
                if (TriplePatternUtils.isVariable(key.getSubject())) {
                  solutionMapping.addMapping(key.getSubject(), vertex._2.asInstanceOf[VertexAttribute].getAttribute())
                }

                if (TriplePatternUtils.isVariable(key.getPredicate())) {
                  solutionMapping.addMapping(key.getPredicate(), resultValue.getEdgeAttribute())
                }

                if (TriplePatternUtils.isVariable(key.getObject())) {
                  solutionMapping.addMapping(key.getObject(), resultValue.getConnectedNodeAttribute())
                }

                result = (solutionMapping :: result)
              })
            }
          })
        }
      })

      // Return result if there is at least one
      if (result != null && result.size > 0) {
        result
      } else {
        Iterator.empty
      }
    })
  }
}