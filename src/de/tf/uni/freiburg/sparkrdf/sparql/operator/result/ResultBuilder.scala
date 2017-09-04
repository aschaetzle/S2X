package de.tf.uni.freiburg.sparkrdf.sparql.operator.result

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
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
import de.tf.uni.freiburg.sparkrdf.model.rdf.result._
import de.tf.uni.freiburg.sparkrdf.model.graph.edge.EdgeAttribute
import de.tf.uni.freiburg.sparkrdf.constants.Const
import org.apache.spark.storage.StorageLevel

/**
 * Builds the solutions mappings from the output of the basic graph pattern matching
 *
 * @author Thorsten Berberich
 */
trait ResultBuilder {

  /**
   * Build the result out of the graph. The steps are:
   *
   * 1. Extract the results out of the vertices from the graph
   * 2. Join the results to get the solution mappings
   * 3. Do a cartesian product between all solution mappings
   */
  protected def buildResult(triples: Broadcast[java.util.List[TriplePattern]], vertices: RDD[(VertexId, VertexInterface)]): RDD[SolutionMapping] = {
    val toSearch: java.util.Set[TriplePattern] = TriplePatternUtils.getTriplePatternsForResult(triples.value);

    /*
     * Step 1: Build the initial solution mappings out of the results from the graph vertices
     */
    val vertexResults: RDD[SolutionMapping] = InitialSolutionMappingBuilder.findVertexResultNodes(toSearch, vertices).persist(StorageLevel.MEMORY_AND_DISK)
    vertices.unpersist(true)

    /*
     * Step 2: Join the solution mappings to get the partial results
     */
    val partialResults = TriplePatternUtils.getSortedPartialResults(toSearch);

    /*
     * Step 3: Do a cartesian prodcut between all partial results
     */
    val allPartialResults = SolutionMappingBuilder.joinSolutionMappings(partialResults, vertexResults)

    vertexResults.unpersist(true)

    // RDD which contain the final results, all solution mappings
    var finalResult: RDD[SolutionMapping] = null;

    allPartialResults.foreach(result => {

      if (finalResult == null) {
        // Use the first partial solution mapping in the first round

        val isEmpty = result.mapPartitions(iter => Iterator(!iter.hasNext), true).reduce(_ && _)
        if (!isEmpty) {
          finalResult = result;
        }
      } else {

        val isEmpty = result.mapPartitions(iter => Iterator(!iter.hasNext), true).reduce(_ && _)

        if (!isEmpty) {
          val oldFinalResult = finalResult

          val keyFinalResult = finalResult.map(sol => (1, sol))
          val keyResult = result.map(sol => (1, sol))

          finalResult = keyFinalResult.join(keyResult).map(res => {
            res._2._1.addAllMappings(res._2._2.getAllMappings())
            res._2._1
          })

          /*
           * Approach with cartesian product which only works for smaller data 
           */
          //          // Do the cartesian product between the rest of the partial solution mappings
          //          finalResult = finalResult.cartesian(result).map(cartesian => {
          //            cartesian._1.addAllMappings(cartesian._2.getAllMappings())
          //            cartesian._1
          //          })
          //
          //          if (finalResult.partitions.size > 200) {
          //            finalResult = finalResult.coalesce(108, false)
          //          }
          //          finalResult.persist(StorageLevel.MEMORY_AND_DISK)

          finalResult.persist(StorageLevel.MEMORY_AND_DISK)
          oldFinalResult.unpersist(true)
          result.unpersist(true)
        }
      }
    })

    finalResult
  }

}