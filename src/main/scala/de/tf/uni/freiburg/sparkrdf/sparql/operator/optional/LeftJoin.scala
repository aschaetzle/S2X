package de.tf.uni.freiburg.sparkrdf.sparql.operator.optional

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import scala.collection.JavaConversions._
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern
import de.tf.uni.freiburg.sparkrdf.model.graph.node._
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple._
import de.tf.uni.freiburg.sparkrdf.sparql.message._
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
import de.tf.uni.freiburg.sparkrdf.model.rdf.result._
import de.tf.uni.freiburg.sparkrdf.model.graph.edge.EdgeAttribute
import com.hp.hpl.jena.sparql.engine.main.LeftJoinClassifier
import de.tf.uni.freiburg.sparkrdf.constants.Const

/**
 * SPARQL Left join operator or optional
 *
 * @author Thorsten Berberich
 */
trait LeftJoin {

  /**
   * Execute an left join
   */
  protected def joinLeft(joinVars: Broadcast[java.util.List[String]], left: RDD[SolutionMapping], right: RDD[SolutionMapping]): RDD[SolutionMapping] = {
    if (left == null && right == null) {
      return null
    } else if (left == null && right != null) {
      return null
    } else if (left != null && right == null) {
      return left
    }

    var result: RDD[SolutionMapping] = null

    if (joinVars.value == null || joinVars.value.isEmpty()) {
      val cartesian = left.cartesian(right)
      left.unpersist(true)
      right.unpersist(true)

      result = cartesian.map(result => {
        result._1.addAllMappings(result._2.getAllMappings());
        result._1
      })
    } else {

      val leftPair: RDD[(String, SolutionMapping)] = left.map(solution => {

        var joinKey: String = ""
        joinVars.value.foreach(joinVariable => {
          joinKey = joinKey + solution.getValueToField(joinVariable)
        })

        (joinKey, solution)
      })
      left.unpersist(true)

      val rightPair: RDD[(String, SolutionMapping)] = right.map(solution => {
        var joinKey: String = ""
        joinVars.value.foreach(joinVariable => {
          joinKey = joinKey + solution.getValueToField(joinVariable)
        })
        (joinKey, solution)
      })
      right.unpersist(true)

      val leftJoined: RDD[(String, (SolutionMapping, Option[SolutionMapping]))] = leftPair.leftOuterJoin(rightPair);

      result = leftJoined.map(result => {
        val leftMapping = result._2._1
        result._2._2 match {
          case Some(rightMapping) => leftMapping.addAllMappings(rightMapping.getAllMappings())
          case None => leftMapping;
        }
        leftMapping
      })
    }

    if (result.partitions.size > 108) {
      result = result.coalesce(108, false)
    }
    result.persist(Const.STORAGE_LEVEL)
    result
  }
}