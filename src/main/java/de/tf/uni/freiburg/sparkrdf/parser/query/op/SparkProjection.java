package de.tf.uni.freiburg.sparkrdf.parser.query.op;

import java.util.HashSet;
import java.util.Set;

import org.apache.spark.rdd.RDD;

import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;

import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class SparkProjection implements SparkOp {

    private final OpProject op;
    private final String TAG = "Projection";

    public SparkProjection(OpProject op) {
	this.op = op;
    }

    @Override
    public void execute() {
	if (op.getSubOp() != null) {
	    int resID = op.getSubOp().hashCode();
	    RDD<SolutionMapping> res = IntermediateResultsModel.getInstance()
		    .getResultRDD(resID);
	    Set<String> variables = new HashSet<>();
	    for (Var v : op.getVars()) {
		variables.add("?" + v.getVarName());
	    }
	    RDD<SolutionMapping> result = SparkFacade.projectResults(variables,
		    res);
	    IntermediateResultsModel.getInstance().putResult(op.hashCode(),
		    result, variables);
	    IntermediateResultsModel.getInstance().removeResult(resID);
	}
    }

    @Override
    public String getTag() {
	return TAG;
    }

}
