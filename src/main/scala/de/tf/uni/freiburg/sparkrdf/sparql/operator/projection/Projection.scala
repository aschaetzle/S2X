package de.tf.uni.freiburg.sparkrdf.sparql.operator.projection

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

/**
 * Execute a projection on the solution mappings
 *
 * @author Thorsten Berberich
 */
trait Projection {

  /**
   * Execute the projection
   */
  protected def projection(vars: Broadcast[java.util.Set[String]], results: RDD[SolutionMapping]): RDD[SolutionMapping] = {
    if (results == null) {
      return null
    }

    val resultProjected: RDD[SolutionMapping] = results.map(result => {
      result.project(vars.value)
      result
    })

    resultProjected.persist(Const.STORAGE_LEVEL)
    results.unpersist(true)
    resultProjected
  }

}