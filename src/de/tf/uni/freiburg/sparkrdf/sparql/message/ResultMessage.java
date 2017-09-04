package de.tf.uni.freiburg.sparkrdf.sparql.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;

/**
 * Class where all the results are send from one node to another
 * 
 * @author Thorsten Berberich
 * 
 */
public class ResultMessage implements IMessage {

    /**
     * Generated serial
     */
    private static final long serialVersionUID = -8482329360672360550L;

    /**
     * {@link ResultValue}s for the children of a node
     */
    private Map<CompositeKeyNodeAttr, List<ResultValue>> childResults;

    /**
     * {@link ResultValue}s for the parents of a node
     */
    private Map<CompositeKeyNodeAttr, List<ResultValue>> parentResults;

    /**
     * Set of node Ids where the results should be removed
     */
    private Set<String> toRemove;

    /**
     * Updated results for the node
     */
    private Map<CompositeKey, List<ResultValue>> nodeUpdateResults = null;

    /**
     * Add a one result to the child results
     * 
     * @param key
     *            Field the subject node is
     * @param value
     *            {@link ResultValue}
     */
    public void addSingleChildResult(CompositeKeyNodeAttr key, ResultValue value) {
	if (childResults == null) {
	    childResults = new HashMap<>();
	}
	if (childResults.get(key) == null) {
	    List<ResultValue> results = new ArrayList<>();
	    results.add(value);
	    childResults.put(key, results);
	} else {
	    childResults.get(key).add(value);
	}
    }

    @Override
    public void addChildResults(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException {
	if (toAdd != null) {
	    if (childResults == null) {
		childResults = new HashMap<>();
	    }
	    addResultsToMap(childResults, toAdd);
	}
    }

    @Override
    public void addParentResults(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException {
	if (toAdd != null) {
	    if (parentResults == null) {
		parentResults = new HashMap<>();
	    }
	    addResultsToMap(parentResults, toAdd);
	}
    }

    /**
     * Adds the results to the given map
     * 
     * @param map
     *            The map where the results are added to
     * @param toAdd
     *            Results to add
     */
    private void addResultsToMap(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> map,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd) {
	for (Entry<CompositeKeyNodeAttr, List<ResultValue>> results : toAdd
		.entrySet()) {
	    // There is a list for this key
	    if (map.containsKey(results.getKey())) {
		map.get(results.getKey()).addAll(results.getValue());
	    } else {
		// No list existing
		map.put(results.getKey(), results.getValue());
	    }
	}
    }

    @Override
    public Map<CompositeKeyNodeAttr, List<ResultValue>> getChildResults() {
	return childResults;
    }

    @Override
    public Boolean isExpandable() {
	return true;
    }

    @Override
    public Map<CompositeKeyNodeAttr, List<ResultValue>> getParentResults() {
	return parentResults;
    }

    @Override
    public Boolean hasChildResults() {
	return !(childResults == null || childResults.isEmpty());
    }

    @Override
    public Boolean hasParentResults() {
	return !(parentResults == null || parentResults.isEmpty());
    }

    @Override
    public String toString() {
	return "ResultMessage [childResults=" + childResults
		+ ", parentResults=" + parentResults + ", toRemove=" + toRemove
		+ "]";
    }

    @Override
    public void addRemoveIds(Set<String> ids)
	    throws UnsupportedOperationException {
	if (toRemove == null) {
	    toRemove = new HashSet<String>();
	}
	toRemove.addAll(ids);
    }

    @Override
    public Set<String> getRemoveAttributes()
	    throws UnsupportedOperationException {
	return toRemove;
    }

    @Override
    public Boolean hasRemoveAttributes() throws UnsupportedOperationException {
	if (toRemove != null && !toRemove.isEmpty()) {
	    return true;
	} else {
	    return false;
	}
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result
		+ ((childResults == null) ? 0 : childResults.hashCode());
	result = prime * result
		+ ((parentResults == null) ? 0 : parentResults.hashCode());
	result = prime * result
		+ ((toRemove == null) ? 0 : toRemove.hashCode());
	return result;
    }

    @Override
    public Map<CompositeKey, List<ResultValue>> getResults() {
	return nodeUpdateResults;
    }

    @Override
    public Boolean hasResults() {
	return true;
    }

    @Override
    public void addResults(Map<CompositeKey, List<ResultValue>> res)
	    throws UnsupportedOperationException {
	this.nodeUpdateResults = res;
    }
}
