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
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util._
import de.tf.uni.freiburg.sparkrdf.constants.Const

/**
 * Scala object to create the final solution mappings
 *
 * @author Thorsten Berberich
 */
object SolutionMappingBuilder {

  /**
   * Joins the solution mappings together to create the final solution mappings
   */
  def joinSolutionMappings(partialResults: java.util.List[PartialResult],
    vertexResults: RDD[SolutionMapping]): scala.collection.mutable.Buffer[RDD[SolutionMapping]] = {

    // Build the solution mappings for every partial result
    partialResults.map(partialResult => {

      var firstRound = true
      var solutionMappingForPartialResult: RDD[SolutionMapping] = null

      partialResult.getAllTriples().foreach(triplePattern => {

        // First round
        if (firstRound) {

          // Filter out the solutions for the first triple pattern
          solutionMappingForPartialResult = vertexResults.filter(result => result.getInitialTriplePattern().equals(triplePattern.getStringRepresentation()))
          firstRound = false

          solutionMappingForPartialResult.persist(Const.STORAGE_LEVEL)

          // Every other round, if there were results
        } else {

          val isEmpty = solutionMappingForPartialResult.mapPartitions(iter => Iterator(!iter.hasNext), true).reduce(_ && _)

          if (!isEmpty) {

            val oldSolutionMapping = solutionMappingForPartialResult

            // Search the results to join
            val toJoin = vertexResults.filter(result => result.getInitialTriplePattern().equals(triplePattern.getStringRepresentation()))

            // Get all the stored variables from the results that exists until now
            val firstSolutionMapping = solutionMappingForPartialResult.first

            // Calculate the join variables
            val joinVariables = TriplePatternUtils.getJoinVariables(firstSolutionMapping.getStoredVariables(), triplePattern)

            /* 
           * Convert the RDDs to pair rdds that they can be joined. The join 
           * key is the concatenation of the values from the join variables.
           */
            val firstJoin = solutionMappingForPartialResult.map(result => {
              (getJoinKey(joinVariables, result), result)
            })

            val secondJoin = toJoin.map(result => {
              (getJoinKey(joinVariables, result), result)
            })

            // Do the join
            val join = firstJoin.join(secondJoin);

            // Add all new mappings to the result
            solutionMappingForPartialResult = join.map(joined => {
              val sol: SolutionMapping = new SolutionMapping(joined._2._1.getInitialTriplePattern());
              sol.addAllMappings(joined._2._1.getAllMappings())
              sol.addAllMappings(joined._2._2.getAllMappings())
              sol
            }).persist(Const.STORAGE_LEVEL)

            oldSolutionMapping.unpersist(true)

          }
        }

      })

      solutionMappingForPartialResult.persist(Const.STORAGE_LEVEL)
    })
  }

  /**
   * Create the join key from the given variables and the solution mapping out of the values from the variable
   */
  private def getJoinKey(joinVariables: java.util.List[String], solMapping: SolutionMapping): String = {
    var joinKey: String = ""
    joinVariables.foreach(joinVariable => {
      joinKey = joinKey + solMapping.getValueToField(joinVariable)
    })
    joinKey
  }

}