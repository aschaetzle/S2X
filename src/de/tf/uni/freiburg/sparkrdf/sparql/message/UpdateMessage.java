package de.tf.uni.freiburg.sparkrdf.sparql.message;

import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;

public class UpdateMessage implements IMessage {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -1725207141775020009L;

    Map<CompositeKey, List<ResultValue>> results;

    public UpdateMessage(Map<CompositeKey, List<ResultValue>> res) {
	this.results = res;
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
    public Map<CompositeKey, List<ResultValue>> getResults() {
	return results;
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
    public Set<String> getRemoveAttributes()
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
    }

    @Override
    public Boolean hasRemoveAttributes() throws UnsupportedOperationException {
	return false;
    }

    @Override
    public void addRemoveIds(Set<String> attr)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
    }

    @Override
    public Boolean hasResults() {
	return true;
    }

    @Override
    public void addResults(Map<CompositeKey, List<ResultValue>> res)
	    throws UnsupportedOperationException {
	throw new UnsupportedOperationException();
    }

}
