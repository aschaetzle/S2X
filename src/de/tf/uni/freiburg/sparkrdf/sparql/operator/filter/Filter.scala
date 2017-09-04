package de.tf.uni.freiburg.sparkrdf.sparql.operator.filter

import de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.IExpression
import org.apache.spark.rdd.RDD
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
import org.apache.spark.broadcast.Broadcast
import scala.collection.JavaConversions._
import de.tf.uni.freiburg.sparkrdf.constants.Const

/**
 * SPARQL Filter operator
 *
 * @author Thorsten Berberich
 */
trait Filter {

  /**
   * Filter the result by the given expressions
   */
  protected def filterResult(result: RDD[SolutionMapping], exprs: Broadcast[java.util.Set[IExpression]]): RDD[SolutionMapping] = {
    if (result == null) {
      return null;
    }

    val filtered = result.filter(solution => {
      var res: Boolean = true;
      exprs.value.foreach(expr => {
        res = res && expr.evaluate(solution)
      })
      res
    })
    filtered.persist(Const.STORAGE_LEVEL)
    result.unpersist(true)

    filtered
  }

}