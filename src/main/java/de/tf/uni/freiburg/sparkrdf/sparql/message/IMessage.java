package de.tf.uni.freiburg.sparkrdf.sparql.message;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;

/**
 * Interface for the result values of a node and the ones that are sent.
 * 
 * @author Thorsten Berberich
 * 
 */
public interface IMessage extends Serializable {

    /**
     * Add all results to this result for the child nodes
     * 
     * @param toAdd
     *            Set of {@link ResultValue}s to add
     */
    public void addChildResults(
            Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException;

    /**
     * Add all results to this result for the parent nodes
     * 
     * @param toAdd
     *            Set of {@link ResultValue}s to add
     */
    public void addParentResults(
            Map<CompositeKeyNodeAttr, List<ResultValue>> toAdd)
	    throws UnsupportedOperationException;

    /**
     * Get all results for the child nodes
     * 
     * @return Set of all results
     */
    public Map<CompositeKeyNodeAttr, List<ResultValue>> getChildResults();

    /**
     * Get all results for the parent nodes
     * 
     * @return Set of all results
     */
    public Map<CompositeKeyNodeAttr, List<ResultValue>> getParentResults();

    /**
     * Get the results for the node. Only applicable for {@link UpdateMessage}
     * and {@link NodeResultMessage}.
     * 
     * @return Results for one node
     */
    public Map<CompositeKey, List<ResultValue>> getResults();

    /**
     * Check if this messages has results
     * 
     * @return True iff it has results
     */
    public Boolean hasResults();

    /**
     * Add results to this message, works only once. If you call it twice then
     * the last results will be taken
     * 
     * @param res
     *            The result
     */
    public void addResults(Map<CompositeKey, List<ResultValue>> res)
	    throws UnsupportedOperationException;

    /**
     * Check if a message has results from the children of a node
     * 
     * @return True iff results from children are available
     */
    public Boolean hasChildResults();

    /**
     * Check if a message has results from the parents of a node
     * 
     * @return True iff results from parents are available
     */
    public Boolean hasParentResults();

    /**
     * Checks if this datatype can store more than one result
     * 
     * @return True iff more than one result can be stored, false otherwise
     */
    public Boolean isExpandable();

    /**
     * Get the attribute of the node to remove the results
     * 
     * @return NodeTest attribute
     */
    public Set<String> getRemoveAttributes()
	    throws UnsupportedOperationException;

    /**
     * Check if this message has attributes to remove
     * 
     * @return True iff there are attributes
     * @throws UnsupportedOperationException
     */
    public Boolean hasRemoveAttributes() throws UnsupportedOperationException;

    /**
     * Add an node Id where the results should be removed
     * 
     * @param attr
     *            Attribute to remove
     * @throws UnsupportedOperationException
     *             Thrown whenever this operation is called on an object which
     *             is not an instance of {@link ResultMessage}
     */
    public void addRemoveIds(Set<String> attr)
	    throws UnsupportedOperationException;
}
