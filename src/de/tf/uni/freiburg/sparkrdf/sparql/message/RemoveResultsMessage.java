package de.tf.uni.freiburg.sparkrdf.sparql.message;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;

public class RemoveResultsMessage implements IMessage {

    /**
     * Generated serial UID
     */
    private static final long serialVersionUID = -1481383499264457998L;

    /**
     * The node attribute where the results should be removed
     */
    private final String nodeAttribute;

    /**
     * Create a new message for a node where the results should be removed
     * 
     * @param nodeAttribute
     *            Attribute of the node where the results should be removed
     */
    public RemoveResultsMessage(String nodeAttribute) {
	this.nodeAttribute = nodeAttribute;
    }

    /**
     * Get the attribute of the node where the results should be removed
     * 
     * @return Attribute of the node
     */
    public String getNodeAttribute() {
	return nodeAttribute;
    }

    @Override
    public void addChildResults(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
    }

    @Override
    public void addParentResults(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
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
	return false;
    }

    @Override
    public void addRemoveIds(Set<String> id)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> getRemoveAttributes()
	    throws UnsupportedOperationException {
	Set<String> result = new HashSet<String>();
	result.add(nodeAttribute);
	return result;
    }

    @Override
    public String toString() {
	return "RemoveResultsMessage [nodeAttribute=" + nodeAttribute + "]";
    }

    @Override
    public Boolean hasRemoveAttributes() throws UnsupportedOperationException {
	return true;
    }

    @Override
    public Map<CompositeKey, List<ResultValue>> getResults() {
	return null;
    }

    @Override
    public Boolean hasResults() {
	return false;
    }

    @Override
    public void addResults(Map<CompositeKey, List<ResultValue>> res)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
    }

}
