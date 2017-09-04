package de.tf.uni.freiburg.sparkrdf.sparql.message;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;
import de.tf.uni.freiburg.sparkrdf.sparql.message.datatype.AdjacentNode;

public class AdjacentNodeMessage implements IMessage {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = 740238242622003464L;

    /**
     * Parent and child relationships for a node
     */
    private List<AdjacentNode> relationships;

    /**
     * Creates a new object and stores the given relationship
     * 
     * @param relationship
     *            New relationship to store
     */
    public AdjacentNodeMessage(AdjacentNode relationship) {
	relationships = new ArrayList<>();
	relationships.add(relationship);
    }

    /**
     * Creates a new empty object
     */
    public AdjacentNodeMessage() {
    };

    /**
     * Add adjacent nodes to the list of this object
     * 
     * @param relationships
     *            Adjacent nodes to add
     */
    public void addAdjacentNodes(List<AdjacentNode> relationships) {
	if (this.relationships == null) {
	    this.relationships = new ArrayList<>();
	}
	this.relationships.addAll(relationships);
    }

    /**
     * Get all stored adjacent nodes from this object
     * 
     * @return All adjacent nodes that are stored
     */
    public List<AdjacentNode> getAdjacentNodes() {
	return relationships;
    }

    @Override
    public void addChildResults(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException(
		"Not supported by the AdjacentMessage-Type");
    }

    @Override
    public void addParentResults(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException(
		"Not supported by the AdjacentMessage-Type");
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
		"Not supported by the AdjacentMessage-Type");
    }

    @Override
    public Boolean hasRemoveAttributes() throws UnsupportedOperationException {
	return false;
    }

    @Override
    public void addRemoveIds(Set<String> attr)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException(
		"Not supported by the AdjacentMessage-Type");
    }

    @Override
    public String toString() {
	return "AdjacentNodeMessage [relationships=" + relationships + "]";
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
