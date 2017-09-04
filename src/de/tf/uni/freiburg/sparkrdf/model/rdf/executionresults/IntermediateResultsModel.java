package de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.spark.rdd.RDD;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * Class that stores all intermediate results
 * 
 * @author Thorsten Berberich
 * 
 */
public class IntermediateResultsModel {

    /**
     * All intermediate results
     */
    private static Map<Integer, RDDVariablePair> results;

    /**
     * Singleton instance
     */
    private static IntermediateResultsModel instance;

    /**
     * Private constructor because of the singleton
     */
    private IntermediateResultsModel() {
	results = new HashMap<Integer, RDDVariablePair>();
    }

    /**
     * Constructor to get the only instance of this class
     * 
     * @return
     */
    public static IntermediateResultsModel getInstance() {
	if (instance == null) {
	    instance = new IntermediateResultsModel();
	}
	return instance;
    }

    /**
     * Put a result into the map
     * 
     * @param ID
     *            ID of the result
     * @param result
     *            The result
     * @param vars
     *            Variables used by the result
     */
    public void putResult(int ID, RDD<SolutionMapping> result, Set<String> vars) {
	results.put(ID, new RDDVariablePair(result, vars));
    }

    /**
     * Get the result for the id
     * 
     * @param ID
     *            id of the result
     * @return The result for the id
     */
    public RDD<SolutionMapping> getResultRDD(int ID) {
	return results.get(ID).getRdd();
    }

    /**
     * Get the variables of a result
     * 
     * @param ID
     *            id of the result
     * @return Set of variables which are used by the result
     */
    public Set<String> getResultVariables(int ID) {
	return results.get(ID).getVariables();
    }

    /**
     * Remove a result
     * 
     * @param ID
     *            Id of the result to be removed
     */
    public void removeResult(int ID) {
	results.remove(ID);
    }

    /**
     * Delete all results
     */
    public void clearResults() {
	results.clear();
    }

    /**
     * Get the final result
     * 
     * @return One result of the map or throws an UnsupportedOperationException
     *         if more than one result is in the map
     */
    public RDD<SolutionMapping> getFinalResult() {
	if (results.size() > 1) {
	    throw new UnsupportedOperationException(
		    "Model contains more than one result, so no final result found");
	} else {
	    for (Entry<Integer, RDDVariablePair> entry : results.entrySet()) {
		return entry.getValue().getRdd();
	    }
	}
	return null;
    }
}
