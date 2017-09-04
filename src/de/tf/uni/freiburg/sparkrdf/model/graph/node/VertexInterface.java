package de.tf.uni.freiburg.sparkrdf.model.graph.node;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;

/**
 * Interface for all nodes in the graph, which is introduced that the graph can
 * be joined with other types
 * 
 * @author Thorsten Berberich
 * 
 */
public interface VertexInterface extends Serializable {

    /**
     * Add {@link ResultValue}s that are fulfilled by the children of this node
     * 
     * @param childMatches
     *            Matching results
     */
    public void addChildMatches(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> childMatches);

    /**
     * Check if this node is matching some part of the basic graph pattern
     * 
     * @return true iff this node matches at least one part of the basic graph
     *         pattern
     */
    public Boolean isMatching();

    /**
     * Add {@link ResultValue}s that are fulfilled by the parents of this node
     * 
     * @param parentMatches
     *            Matching results
     */
    public void addParentMatches(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> parentMatches);

    /**
     * Adds all results to this node
     * 
     * @param results
     *            Results to add
     */
    public void addResults(Map<CompositeKey, List<ResultValue>> results);

    /**
     * Get the Ids of the parents of this node
     * 
     * @return List of id
     */
    public Set<Long> getParentIDs();

    /**
     * Get the Ids of the children of this node
     * 
     * @return List of id
     */
    public Set<Long> getChildIDs();

    /**
     * Transform the normal results into result which contain the node attribute
     * that they can be sent to the neighbors
     * 
     * @param id
     *            Id of the node where the results should be sent
     * @param bgp
     *            Basic graph pattern
     * @return Transformed result which contain the node attribute
     */
    public Map<CompositeKeyNodeAttr, List<ResultValue>> getSendResults(Long id,
	    List<TriplePattern> bgp);

    /**
     * Replace all results by the given one.
     * 
     * @param newResults
     *            The new results of the node after this operation
     */
    // public void replaceResults(Map<CompositeKey, List<ResultValue>>
    // newResults);

    /**
     * Verify all results of the node
     * 
     * @param bgp
     *            All {@link TriplePattern} that should be used to verify the
     *            results
     * @param local
     *            True if only things should be verified where no child or
     *            parent results are needed
     * @return True if something was changed in the result
     */
    public Boolean verifyNodeMatches(List<TriplePattern> bgp, Boolean local);

    /**
     * Updates the matching results for the given NodeTest. Old results are
     * removed and all new added.
     * 
     * @param updatedChildMatches
     *            New matches for the given node
     */
    public void updateChildMatches(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> updatedChildMatches);

    /**
     * Updates the results of the node.
     * 
     * @param New
     *            Results or null if the results are empty
     */
    public void updateNodeResults(
	    Map<CompositeKey, List<ResultValue>> nodeResults);

    /**
     * Updates the matching results for the given NodeTest. Old results are
     * removed and all new added.
     * 
     * @param updatedParentMatches
     *            New matches for the given node
     */
    public void updateParentMatches(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> updatedParentMatches);

    /**
     * Get the results of the node
     * 
     * @return All results of the node
     */
    public Map<CompositeKey, List<ResultValue>> getResults();

    /**
     * Check if this node has parent nodes
     * 
     * @return true iff there is at least one parent node
     */
    public Boolean hasParentNodes();

    /**
     * Check if this node has child nodes
     * 
     * @return true iff there is at least one child node
     */
    public Boolean hasChildNodes();

    /**
     * Make a deep copy of this node
     * 
     * @return The copy of this node
     */
    public VertexInterface deepCopy();

    /**
     * Add a parent to this node
     * 
     * @param id
     *            Id of the parent
     * @param attribute
     *            Attribute of the parent
     */
    public void addParentNode(Long id, String attribute);

    /**
     * Add a child to this node
     * 
     * @param id
     *            Id of the child
     * @param attribute
     *            Attribute of the child
     */
    public void addChildNode(Long id, String attribute);

    /**
     * Removes all results of a node from the child and parent match sets and
     * also deletes them from the parents and children
     * 
     * @param nodeAttr
     *            Attribute to remove
     */
    public void removeAdjacentMatches(String nodeAttr);
}
