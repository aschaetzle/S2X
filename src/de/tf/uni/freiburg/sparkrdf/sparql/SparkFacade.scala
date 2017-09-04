package de.tf.uni.freiburg.sparkrdf.sparql

import de.tf.uni.freiburg.sparkrdf.sparql.operator._
import collection.mutable.Buffer
import de.tf.uni.freiburg.sparkrdf.model.graph.GraphLoader
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp._
import de.tf.uni.freiburg.sparkrdf.parser.rdf._
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.ResultBuilder
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import de.tf.uni.freiburg.sparkrdf.model.graph.node._
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
import de.tf.uni.freiburg.sparkrdf.sparql.operator.order.Order
import de.tf.uni.freiburg.sparkrdf.sparql.operator.projection.Projection
import de.tf.uni.freiburg.sparkrdf.sparql.operator.optional.LeftJoin
import de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.IExpression
import de.tf.uni.freiburg.sparkrdf.sparql.operator.filter.Filter
import com.hp.hpl.jena.shared.PrefixMapping
import de.tf.uni.freiburg.sparkrdf.constants.Const
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * Class that calls all spark methods, such that all method calls from java go through this class.
 *
 * @author Thorsten Berberich
 */
object SparkFacade extends BasicGraphPattern with ResultBuilder with Projection with LeftJoin with Filter with Order {

  /**
   * Spark context
   */
  private var context: SparkContext = null;

  /**
   * Creates a new Spark context
   */
  def createSparkContext() {
    val conf = new SparkConf()

    /*
     * See the configuration of Spark:
     * https://spark.apache.org/docs/1.2.0/configuration.html
     */

    // Use the Kryo serializer, because it is faster than Java serializing
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "de.tf.uni.freiburg.sparkrdf.sparql.serialization.Registrator")
    conf.set("spark.core.connection.ack.wait.timeout", "5000");
    conf.set("spark.shuffle.consolidateFiles", "true");
    conf.set("spark.rdd.compress", "true");
    conf.set("spark.kryoserializer.buffer.max.mb", "512");

    if (Const.locale) {
      conf.setMaster("local")
    }

    if (Const.executorMem != null) {
      conf.set("spark.executor.memory", Const.executorMem)
    }

    if (Const.parallelism != null) {
      conf.set("spark.default.parallelism", Const.parallelism)
    }

    if (Const.memoryFraction != null) {
      conf.set("spark.storage.memoryFraction", Const.memoryFraction)
    }

    if (Const.jobName != null) {
      conf.setAppName(Const.jobName)
    } else {
      conf.setAppName("SPARQL for SPARK Graph: \"" + Const.inputFile + "\"")
    }

    context = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }

  /**
   * Loads the graph into the graph model, if it isn't loaded yet
   */
  def loadGraph() {
    val conf: Configuration = new Configuration();
    val fs: FileSystem = FileSystem.get(conf);

    if (!fs.exists(new Path(fs.getHomeDirectory().toString() +
      "/" + Const.inputFile.replace("\\W", "_")))) {

      if (Const.countBasedLoading) {
        GraphLoader.loadGraphCountBased(Const.inputFile, context);
      } else {
        GraphLoader.loadGraphHashBased(Const.inputFile, context);
      }

    } else {
      GraphLoader.loadPreloadedGraph(Const.inputFile, context)
    }
  }

  /**
   * Executes a basic graph pattern on the loaded graph
   */
  def executeBasicGraphPattern(triples: java.util.List[TriplePattern]): RDD[(VertexId, VertexInterface)] = {
    // Broadcast all Sparql triples to all workers
    val broadcastedTriples: Broadcast[java.util.List[TriplePattern]] = context.broadcast(triples)
    val result = matchBGP(broadcastedTriples, GraphLoader.getGraph, context)
    broadcastedTriples.unpersist(true)
    result
  }

  /**
   * Build the results out of the graph from the graph model
   */
  def buildResult(triples: java.util.List[TriplePattern], res: RDD[(VertexId, VertexInterface)]): RDD[SolutionMapping] = {
    // Broadcast all Sparql triples to all workers
    val broadcastedTriples: Broadcast[java.util.List[TriplePattern]] = context.broadcast(triples)
    val result = buildResult(broadcastedTriples, res)
    broadcastedTriples.unpersist(true)
    result
  }

  /**
   * Build the results out of the graph from the graph model
   */
  def projectResults(vars: java.util.Set[String], res: RDD[SolutionMapping]): RDD[SolutionMapping] = {
    // Broadcast all Sparql triples to all workers
    val broadcastVars: Broadcast[java.util.Set[String]] = context.broadcast(vars)
    val result = projection(broadcastVars, res)
    broadcastVars.unpersist(true)
    result
  }

  /**
   * Filter the result
   */
  def filter(res: RDD[SolutionMapping], expr: java.util.Set[IExpression]): RDD[SolutionMapping] = {
    val broadcastExprs: Broadcast[java.util.Set[IExpression]] = context.broadcast(expr)
    val result = filterResult(res, broadcastExprs)
    broadcastExprs.unpersist(true)
    result
  }

  /**
   * Union two RDDs
   */
  def union(left: RDD[SolutionMapping], right: RDD[SolutionMapping]): RDD[SolutionMapping] = {
    if (left == null && right == null) {
      return null
    } else if (left != null && right == null) {
      return left
    } else if (left == null && right != null) {
      return right
    }

    val result = left.union(right).persist(Const.STORAGE_LEVEL)
    left.unpersist(true)
    right.unpersist(true);
    result
  }

  /**
   * Close the Spark context
   */
  def closeContext() {
    context.stop()
  }

  /**
   * Print the RDD to the console
   */
  def printRDD(rdd: RDD[SolutionMapping]) {
    if (rdd != null) {
      rdd.collect.foreach(println)
    }
  }

  /**
   * Count the RDD
   */
  def getRDDCount(rdd: RDD[SolutionMapping]): Long = {
    if (rdd != null) {
      rdd.count
    } else {
      return 0
    }

  }

  /**
   * Save the result to the HDFS file
   */
  def saveResultToFile(rdd: RDD[SolutionMapping]) {
    if (rdd != null) {
      rdd.saveAsTextFile(Const.outputFilePath);
    }
  }

  /**
   * Order the result
   */
  def order(result: RDD[SolutionMapping], variable: String, asc: Boolean): RDD[SolutionMapping] = {
    orderBy(result, variable, asc)
  }

  /**
   * Limit and offset the result
   */
  def limitOffset(result: RDD[SolutionMapping], limit: Int, offset: Int): RDD[SolutionMapping] = {
    if (result == null) {
      return null
    }

    /*
     * Correction if the result is smaller than the limit if the offset is 
     * taken away from the result
     */
    var copyLength = 0

    val resultCount = result.count
    if (resultCount <= limit + offset) {
      /*
       * Calculate how big the part is that is left when the offset is
       * subtracted
       */
      copyLength = (resultCount - offset).toInt
      if (copyLength < 0) {
        // Slice bigger than result
        return null
      }
    } else {
      copyLength = limit
    }

    val limited = result.take(limit + offset)
    val newOffset = new Array[SolutionMapping](copyLength)
    Array.copy(limited, offset, newOffset, 0, copyLength)
    result.unpersist(true)

    val resultRDD = context.parallelize(newOffset).persist(Const.STORAGE_LEVEL)
    resultRDD
  }

  /**
   * Limit the result
   */
  def limit(result: RDD[SolutionMapping], limit: Int): RDD[SolutionMapping] = {
    if (result == null) {
      return null
    }

    val limited = result.take(limit)
    result.unpersist(true)

    val resultRDD = context.parallelize(limited).persist(Const.STORAGE_LEVEL)
    resultRDD
  }

  /**
   * Delete double values in the result
   */
  def distinct(result: RDD[SolutionMapping]): RDD[SolutionMapping] = {
    if (result == null) {
      return null
    }

    result.distinct
  }

  /**
   * Execute a left join
   */
  def optional(joinVars: java.util.List[String], left: RDD[SolutionMapping], right: RDD[SolutionMapping]): RDD[SolutionMapping] = {
    val broadcastVars: Broadcast[java.util.List[String]] = context.broadcast(joinVars)
    val result = joinLeft(broadcastVars, left, right)
    broadcastVars.unpersist(true)
    result
  }

}