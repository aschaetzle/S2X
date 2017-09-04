package de.tf.uni.freiburg.sparkrdf.constants

import org.apache.spark.storage.StorageLevel

/**
 * Constants
 *
 * @author Thorsten Berberich
 */
object Const {

  /**
   * Gives the storage level for every RDD that will be cached.
   * Can be useful for later uses if the storage level can be changed
   * if one is set
   */
  final val STORAGE_LEVEL: StorageLevel = StorageLevel.MEMORY_ONLY

  /**
   * Input HDFS file
   */
  var inputFile: String = null;

  /**
   * Query files, separated by comma
   */
  var query: String = null;

  /**
   * Executor memory
   */
  var executorMem: String = null;

  /**
   * HDFS path for the result
   */
  var outputFilePath: String = null;

  /**
   * Store the output of the graph loading
   */
  var saveLoadOutput: Boolean = false;

  /**
   * Memory fraction which is used for caching
   */
  var memoryFraction: String = null;

  /**
   * Default parallelism
   */
  var parallelism: String = null;

  /**
   * Flag if it should run in locale mode
   */
  var locale: Boolean = false;

  /**
   * Job name
   */
  var jobName: String = null;

  /**
   * The actual parsed query
   */
  var parsedQuery: String = null;

  /**
   * Path where the timings should be stored
   */
  var timeFilePath: String = null;

  /**
   * Flag if the result should be printed to console
   */
  var printToConsole: Boolean = false;

  /**
   * True for WatDiv graphs
   */
  var watDiv: Boolean = false;

  /**
   * Load the graph count based
   */
  var countBasedLoading: Boolean = false;

  /**
   * True if variable predicates are used
   */
  var varPred: Boolean = false;
}