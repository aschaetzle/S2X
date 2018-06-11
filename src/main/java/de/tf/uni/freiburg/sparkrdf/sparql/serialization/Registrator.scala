package de.tf.uni.freiburg.sparkrdf.sparql.serialization

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import de.tf.uni.freiburg.sparkrdf.model.graph.node._
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple._
import de.tf.uni.freiburg.sparkrdf.sparql.message._
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue
import de.tf.uni.freiburg.sparkrdf.model.graph.edge._
import de.tf.uni.freiburg.sparkrdf.parser.rdf._
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr
import de.tf.uni.freiburg.sparkrdf.parser.PrefixUtil

/**
 * Class to register the files which are serialized by the Kryo serializer, to make this more efficient.
 *
 * @author Thorsten Berberich
 */
class Registrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // Nodes
    kryo.register(classOf[VertexAttribute])
    kryo.register(classOf[VertexInterface])

    // Edge attribute
    kryo.register(classOf[EdgeAttribute])

    // RDF query triple
    kryo.register(classOf[TriplePattern])

    // RDF Parser
    kryo.register(classOf[RDFParser])

    // Results
    kryo.register(classOf[IMessage])
    kryo.register(classOf[ResultMessage])
    kryo.register(classOf[ResultValue])
    kryo.register(classOf[RemoveResultsMessage])
    kryo.register(classOf[NodeResultMessage])
    kryo.register(classOf[SolutionMapping])
    kryo.register(classOf[CompositeKey])
    kryo.register(classOf[CompositeKeyNodeAttr])

    // Util
    kryo.register(classOf[PrefixUtil])
  }
}