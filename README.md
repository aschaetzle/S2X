# S2X

S2X (SPARQL on Spark with GraphX) is a SPARQL query processor for Hadoop based on Spark GraphX. It combines graph-parallel abstraction of GraphX to implement the graph pattern matching part of SPARQL with data-parallel computation of Spark to build the results of other SPARQL operators.

http://dbis.informatik.uni-freiburg.de/forschung/projekte/DiPoS/S2X.html

### LICENSE
Unless explicitly stated otherwise all files in this repository are licensed under the Apache Software License 2.0

>   Copyright 2017 University of Freiburg
>
>   Licensed under the Apache License, Version 2.0 (the "License");
>   you may not use this file except in compliance with the License.
>   You may obtain a copy of the License at
>
>       http://www.apache.org/licenses/LICENSE-2.0
>
>   Unless required by applicable law or agreed to in writing, software
>   distributed under the License is distributed on an "AS IS" BASIS,
>   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
>   See the License for the specific language governing permissions and
>   limitations under the License.


## For Users:

```
spark-submit --class de.tf.uni.freiburg.sparkrdf.run.QueryExecutor --master spark://MasterAdress SparqlForSpark-1.0-with-dependencies.jar -i <GRAPH_PATH> -mem <WORKER_MEMORY> -q <QUERY_FILE_PATH_1, QUERY_FILE_PATH_2, ...> -jn <JOB_NAME>
```

Command Line Parameters:

```
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
```

See also the Spark Submit page:
https://spark.apache.org/docs/latest/submitting-applications.html


## For Developers:

The project is a Maven project which is built in Eclipse with Java and Scala classes. The Eclipse Plug-Ins M2E (Version 1.5.0) and the Scala plugin from scala-ide.org (Version 3.0.3v-2_10...) are used. The project is built by Maven where the ouput are two jar files. One has also the dependencies packed inside the file, because some dependencies are not available at the cluster.

To integrate a new SPARQL operator a new class has to be implemented in the package "de.tf.uni.freiburg.sparkrdf.parser.query.op" which implements the interface "SparkOp". In the class "de.tf.uni.freiburg.sparkrdf.parser.query.op.AlgebraTranslator", the new SparkOperator has to be created in the correct method. Also a new Scala trait has to be implemented in the "de.tf.uni.freiburg.sparkrdf.sparql.operator". This trait imeplments the interaction with Apache Spark and GraphX. The "SparkFacade" extends all the traits of the operators and calls the implementation. The last step is to call the implemented trait through the "SparkFacade" from the implemented SparkOp.
