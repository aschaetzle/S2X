package de.tf.uni.freiburg.sparkrdf.parser.query.op;

import java.util.Set;

import org.apache.spark.rdd.RDD;

import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;

import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class SparkDistinct implements SparkOp {

    private final String TAG = "Distinct";
    private final OpDistinct op;

    public SparkDistinct(OpDistinct op) {
	this.op = op;
    }

    @Override
    public void execute() {
	if (op.getSubOp() != null) {
	    RDD<SolutionMapping> oldResult = IntermediateResultsModel
		    .getInstance().getResultRDD(op.getSubOp().hashCode());
	    Set<String> vars = IntermediateResultsModel.getInstance()
		    .getResultVariables(op.getSubOp().hashCode());

	    RDD<SolutionMapping> result = SparkFacade.distinct(oldResult);

	    IntermediateResultsModel.getInstance().removeResult(
		    op.getSubOp().hashCode());
	    IntermediateResultsModel.getInstance().putResult(op.hashCode(),
		    result, vars);
	}
    }

    @Override
    public String getTag() {
	return TAG;
    }

}
