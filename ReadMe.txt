For Users:

spark-submit --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master spark://MasterAdress SparqlForSpark-1.0-with-dependencies.jar -i <GRAPH_PATH> -mem <WORKER_MEMORY> -q <QUERY_FILE_PATH_1, QUERY_FILE_PATH_2, ...> -jn <JOB_NAME>

Command Line Parameters:

-h
	Display help message
-i
	HDFS-Path to the RDF-Graph in N3-format
-q
	Files that contain a SPARQL query (local path)
-o
	HDFS-Path to store SPARQL query result file
-jn
	Name of this job
-mem
	The memory that every executor will use given as JVM memory string (e.g. 512m or 24g). This sets spark.executor.memory
-t
	Local path to the file where the execution duration and the result count of this query should be appended on
-so
	Save the output of the loading as a object file. This file will be loaded the next time you use this graph.
-fr
	Memory to use to cache Spark data. Default: 0.6. This sets spark.storage.memoryFraction
-dp
	Sets spark.default.parallelism
-p
	Print the result to the console. May not work for big results.
-wd
	Replaces the dc: and foaf: prefix to work with WatDiv. Must be used if a WatDiv graph is loaded and if queries on a WatDiv graph are executed.
-l
	Runs the application in local mode. The Spark master will be set to "local"
-countBased
	Load the graph without hashing. Assign the node ids with count based method instead
-vp
	Set this flag if you are planning to use queries where the predicate could be a variable field. Must be set while the first loading of a graph.

See also the Spark Submit page:
https://spark.apache.org/docs/latest/submitting-applications.html
---------------------------------------------------------------------------------------------------

For Developers:

The project is a Maven project which is built in Eclipse with Java and Scala classes. The Eclipse Plug-Ins M2E (Version 1.5.0) and the Scala plugin from scala-ide.org (Version 3.0.3v-2_10...) are used. The project is built by Maven where the ouput are two jar files. One has also the dependencies packed inside the file, because some dependencies are not available at the cluster.

To integrate a new SPARQL operator a new class has to be implemented in the package "de.tf.uni.freiburg.sparkrdf.parser.query.op" which implements the interface "SparkOp". In the class "de.tf.uni.freiburg.sparkrdf.parser.query.op.AlgebraTranslator", the new SparkOperator has to be created in the correct method. Also a new Scala trait has to be implemented in the "de.tf.uni.freiburg.sparkrdf.sparql.operator". This trait imeplments the interaction with Apache Spark and GraphX. The "SparkFacade" extends all the traits of the operators and calls the implementation. The last step is to call the implemented trait through the "SparkFacade" from the implemented SparkOp.