package de.tf.uni.freiburg.sparkrdf.sparql.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;

public class NodeResultMessage implements IMessage {

    /**
     * Generated serial UID
     */
    private static final long serialVersionUID = 2824065044813368362L;

    /**
     * Results of this message
     */
    private final Map<CompositeKey, List<ResultValue>> results;

    /**
     * Create a new {@link NodeResultMessage} with the given key and value.
     * 
     * @param key
     *            {@link CompositeKey}
     * @param value
     *            Results for key
     */
    public NodeResultMessage(CompositeKey key, ResultValue value) {
	List<ResultValue> lst = new ArrayList<ResultValue>();
	lst.add(value);
	results = new HashMap<CompositeKey, List<ResultValue>>();
	results.put(key, lst);
    }

    /**
     * Create an empty message
     */
    public NodeResultMessage() {
	results = new HashMap<>();
    }

    @Override
    public void addResults(Map<CompositeKey, List<ResultValue>> toAdd) {
	for (Entry<CompositeKey, List<ResultValue>> entry : toAdd.entrySet()) {
	    // There is a list for this key
	    if (results.containsKey(entry.getKey())) {
		results.get(entry.getKey()).addAll(entry.getValue());
	    } else {
		// No list existing
		results.put(entry.getKey(), entry.getValue());
	    }
	}
    }

    @Override
    public Map<CompositeKey, List<ResultValue>> getResults() {
	return results;
    }

    @Override
    public void addChildResults(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException(
		"Not applicable for NodeResultMesage");
    }

    @Override
    public void addParentResults(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException(
		"Not applicable for NodeResultMesage");
    }

    @Override
    public Map<CompositeKeyNodeAttr, List<ResultValue>> getChildResults() {
	return null;
    }

    @Override
    public Map<CompositeKeyNodeAttr, List<ResultValue>> getParentResults() {
	return null;
    }

    @Override
    public Boolean hasChildResults() {
	return false;
    }

    @Override
    public Boolean hasParentResults() {
	return false;
    }

    @Override
    public Boolean isExpandable() {
	return true;
    }

    @Override
    public Set<String> getRemoveAttributes()
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException(
		"Not applicable for NodeResultMesage");
    }

    @Override
    public Boolean hasRemoveAttributes() throws UnsupportedOperationException {
	throw new UnsupportedOperationException(
		"Not applicable for NodeResultMesage");
    }

    @Override
    public void addRemoveIds(Set<String> attr)
	    throws UnsupportedOperationException {
    }

    @Override
    public String toString() {
	return "NodeResultMessage [results=" + results + "]";
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((results == null) ? 0 : results.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (!(obj instanceof NodeResultMessage))
	    return false;
	NodeResultMessage other = (NodeResultMessage) obj;
	if (results == null) {
	    if (other.results != null)
		return false;
	} else if (!results.equals(other.results))
	    return false;
	return true;
    }

    @Override
    public Boolean hasResults() {
	return false;
    }

}
