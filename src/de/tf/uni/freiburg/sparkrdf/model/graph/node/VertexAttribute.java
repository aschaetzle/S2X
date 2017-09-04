package de.tf.uni.freiburg.sparkrdf.model.graph.node;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp.util.BGPVerifyUtil;

/**
 * Class that represents one node inside the whole property graph
 * 
 * @author Thorsten Berberich
 * 
 */
public class VertexAttribute implements VertexInterface {

    /**
     * Generated ID
     */
    private static final long serialVersionUID = 2980570399339909277L;

    /**
     * Set with {@link ResultValue}s where this node is in
     */
    Map<CompositeKey, List<ResultValue>> results;

    /**
     * Match set of the child nodes
     */
    Map<CompositeKeyNodeAttr, List<ResultValue>> matchChildren;

    /**
     * Match set of the parent nodes
     */
    Map<CompositeKeyNodeAttr, List<ResultValue>> matchParents;

    /**
     * Parent nodes of this node
     */
    Map<Long, String> parents;

    /**
     * Child nodes of this node
     */
    Map<Long, String> children;

    /**
     * RDF attribute of this node
     */
    private final String nodeAttribute;

    /**
     * Flag if this node is matching some part of the BGP
     */
    private Boolean matching = false;

    /**
     * Creates a new node with the given attribute
     * 
     * @param attribute
     *            RDF attribute of this node
     */
    public VertexAttribute(String attribute) {
	nodeAttribute = attribute;
    }

    @Override
    public void addChildMatches(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> childMatches) {
	if (matching) {
	    matchChildren = childMatches;
	}
    }

    @Override
    public void addParentMatches(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> parentMatches) {
	if (matching) {
	    matchParents = parentMatches;
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
    private <T extends CompositeKey> void addResultsToMap(
	    Map<T, List<ResultValue>> map, Map<T, List<ResultValue>> toAdd) {
	if (map != null && toAdd != null) {
	    for (Entry<T, List<ResultValue>> results : toAdd.entrySet()) {
		// There is a list for this key
		if (map.containsKey(results.getKey())) {
		    map.get(results.getKey()).addAll(results.getValue());
		} else {
		    // No list existing
		    List<ResultValue> results2 = new ArrayList<ResultValue>();
		    results2.addAll(results.getValue());
		    map.put(results.getKey(), results2);
		}
	    }
	}
    }

    @Override
    public void addResults(Map<CompositeKey, List<ResultValue>> results) {
	if (this.results == null) {
	    this.results = new HashMap<>();
	    matching = true;
	}
	addResultsToMap(this.results, results);
    }

    /**
     * Get the Attribute of this node
     * 
     * @return the Attribute
     */
    public String getAttribute() {
	return nodeAttribute;
    }

    @Override
    public Set<Long> getChildIDs() {
	return children.keySet();
    }

    @Override
    public Set<Long> getParentIDs() {
	return parents.keySet();
    }

    @Override
    public Map<CompositeKeyNodeAttr, List<ResultValue>> getSendResults(Long id,
	    List<TriplePattern> bgp) {
	// Map<CompositeKeyNodeAttr, List<ResultValue>> sendRes = new
	// HashMap<>();
	String attribute = null;
	if (parents != null) {
	    attribute = parents.get(id);
	}
	if (attribute == null && children != null) {
	    attribute = children.get(id);
	}

	Map<CompositeKeyNodeAttr, List<ResultValue>> sendRes = new HashMap<>();

	for (Entry<CompositeKey, List<ResultValue>> entry : results.entrySet()) {
	    // Check if the composite key is in a shortcut
	    if (!BGPVerifyUtil.getShortcuts(bgp, entry.getKey()).isEmpty()) {

		/*
		 * Is part of a shortcut, send all results that they can be used
		 * to check the shortcuts
		 */
		sendRes.put(new CompositeKeyNodeAttr(entry.getKey(),
			this.nodeAttribute), entry.getValue());
	    } else {
		/*
		 * Not part of a shortcut, create a list only with the results
		 * that are relevant for the node that will receive this message
		 */
		List<ResultValue> sendResults = new ArrayList<>();
		for (ResultValue result : entry.getValue()) {
		    if (result.getConnectedNodeAttribute().equals(attribute)) {
			sendResults.add(result);
		    }
		}

		if (!sendResults.isEmpty()) {
		    sendRes.put(new CompositeKeyNodeAttr(entry.getKey(),
			    this.nodeAttribute), sendResults);
		}
	    }
	}
	return sendRes;
    }

    @Override
    public Map<CompositeKey, List<ResultValue>> getResults() {
	if (results == null) {
	    return new HashMap<CompositeKey, List<ResultValue>>();
	}
	return results;
    }

    /**
     * Check if this node matches some part of a basic graph pattern
     * 
     * @return True iff the results are not empty and not null
     */
    public Boolean hasResults() {
	if (results != null && !results.isEmpty()) {
	    return true;
	} else {
	    return false;
	}
    }

    @Override
    public Boolean isMatching() {
	return matching;
    }

    @Override
    public void updateChildMatches(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> updatedChildMatches) {
	removeResultsFromMap(matchChildren, updatedChildMatches);

	// Add the new results
	addResultsToMap(matchChildren, updatedChildMatches);
    }

    /**
     * Removes all results that are from nodes which are contained in the
     * toDelete map
     * 
     * @param toClean
     *            Map to clean
     * @param toDelete
     *            Map with results of nodes that should be deleted
     */
    private void removeResultsFromMap(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toClean,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toDelete) {
	if (toClean != null && toDelete != null) {
	    // Get all attributes of the nodes that should be removed
	    Set<String> toRemove = getResultNodeAttributes(toDelete);

	    // Iterate through the result map
	    Iterator<CompositeKeyNodeAttr> iter = toClean.keySet().iterator();

	    while (iter.hasNext()) {
		CompositeKeyNodeAttr entry = iter.next();
		if (toRemove.contains(entry.getNodeAttribute())) {
		    iter.remove();
		}

	    }
	}
    }

    @Override
    public void updateParentMatches(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> updatedParentMatches) {
	removeResultsFromMap(matchParents, updatedParentMatches);

	// Add the new results
	addResultsToMap(matchParents, updatedParentMatches);
    }

    /**
     * Get all distinct node attributes of the results.
     * 
     * @param results
     *            Results to search node attributes
     * @return A set of the node IDs
     */
    private Set<String> getResultNodeAttributes(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> results) {
	Set<String> res = new HashSet<>();
	for (CompositeKeyNodeAttr result : results.keySet()) {
	    res.add(result.getNodeAttribute());
	}
	return res;
    }

    @Override
    public Boolean verifyNodeMatches(List<TriplePattern> bgp, Boolean local) {
	Boolean changed = false;

	// Validate results if they are not null and not validated
	if (results != null) {

	    // Queue with all CompositeKeys that must be checked
	    Queue<CompositeKey> workingQueue = new LinkedList<>(
		    results.keySet());

	    while (!workingQueue.isEmpty() && !results.isEmpty()) {
		CompositeKey actual = workingQueue.poll();

		if (!results.containsKey(actual)) {
		    // Result not existing, nothing to check
		    continue;
		}

		/*
		 * Check that all needed TPs are existing for that field
		 */
		if (!BGPVerifyUtil
			.checkTriplePatternGroup(actual, results, bgp)) {
		    changed = true;
		    deleteAllResultsForField(actual);
		    BGPVerifyUtil.addAllDependingFieldsToQueue(actual,
			    workingQueue, bgp);
		    continue;
		}

		/*
		 * Check that all results are existing in the parent and child
		 * nodes
		 */
		if (!local) {
		    if (!BGPVerifyUtil.checkAdjacentConnections(actual,
			    results, matchParents, matchChildren, bgp,
			    this.nodeAttribute)) {
			changed = true;
			BGPVerifyUtil.addDependingFieldsToQueue(actual,
				workingQueue, bgp);
		    }
		}

		/*
		 * Check the instantiation of the triple pattern
		 */
		if (!BGPVerifyUtil.checkTriplePatternInstantiation(actual,
			results, bgp, this.nodeAttribute)) {
		    changed = true;
		    BGPVerifyUtil.addDependingFieldsToQueue(actual,
			    workingQueue, bgp);
		}

		if (!local) {
		    // Check for shortcuts of length 3
		    if (!BGPVerifyUtil.checkShortcuts(actual, results,
			    matchChildren, matchParents, bgp,
			    this.nodeAttribute)) {
			changed = true;
			BGPVerifyUtil.addDependingFieldsToQueue(actual,
				workingQueue, bgp);

		    }
		}
	    }
	}

	if (results != null && results.isEmpty()) {
	    results = null;
	    matching = false;
	}

	// Set matching flag to false
	if (results == null) {
	    matching = false;
	}

	return changed;
    }

    /**
     * Delete all results for the given field
     * 
     * @param key
     *            {@link CompositeKey} to delete
     */
    private void deleteAllResultsForField(CompositeKey key) {
	String field = key.getField();

	Iterator<CompositeKey> itr = results.keySet().iterator();
	while (itr.hasNext()) {
	    CompositeKey actual = itr.next();
	    if (actual.getField().equals(field)) {
		itr.remove();
	    }
	}
    }

    @Override
    public String toString() {
	String result = "NodeTest [nodeAttribute=" + nodeAttribute
		+ ", matching=" + matching + ", results=\n";
	if (results != null) {
	    for (Entry<CompositeKey, List<ResultValue>> entry : results
		    .entrySet()) {
		result += "\t" + entry.getKey() + "\n";
		for (ResultValue res : entry.getValue()) {
		    result += "\t\t" + res + "\n";
		}
	    }
	} else {
	    result += "empty";
	}

	result += ",\n parents=";

	if (parents != null) {
	    for (Entry<Long, String> val : parents.entrySet()) {
		result += "\n\t" + val.getValue();
	    }
	} else {
	    result += "empty";
	}

	result += ",\n children=";

	if (children != null) {
	    for (Entry<Long, String> val : children.entrySet()) {
		result += "\n\t" + val.getValue();
	    }
	} else {
	    result += "empty";
	}

	result += ",\n matchChildren=";

	if (matchChildren != null) {
	    result += "\n";
	    for (Entry<CompositeKeyNodeAttr, List<ResultValue>> entry : matchChildren
		    .entrySet()) {
		result += "\t" + entry.getKey() + "\n";
		for (ResultValue res : entry.getValue()) {
		    result += "\t\t" + res + "\n";
		}
	    }
	} else {
	    result += "empty";
	}

	result += ",\n matchParents=";

	if (matchParents != null) {
	    result += "\n";
	    for (Entry<CompositeKeyNodeAttr, List<ResultValue>> entry : matchParents
		    .entrySet()) {
		result += "\t" + entry.getKey() + "\n";
		for (ResultValue res : entry.getValue()) {
		    result += "\t\t" + res + "\n";
		}
	    }
	} else {
	    result += "empty";
	}
	result += "\n";
	return result;
    }

    @Override
    public Boolean hasParentNodes() {
	if (parents == null || parents.isEmpty()) {
	    return false;
	} else {
	    return true;
	}
    }

    @Override
    public Boolean hasChildNodes() {
	if (children == null || children.isEmpty()) {
	    return false;
	} else {
	    return true;
	}
    }

    public String toStringShort() {
	String result = "NodeTest [nodeAttribute=" + nodeAttribute
		+ ", matching=" + matching + ", results=\n";
	if (results != null) {
	    for (Entry<CompositeKey, List<ResultValue>> entry : results
		    .entrySet()) {
		result += "\t" + entry.getKey() + "\n";
		for (ResultValue res : entry.getValue()) {
		    result += "\t\t" + res + "\n";
		}
	    }
	} else {
	    result += "empty";
	}
	return result;
    }

    @Override
    public void removeAdjacentMatches(String nodeAttr) {
	removeAttributeFromMatchSet(matchChildren, nodeAttr);
	removeAttributeFromMatchSet(matchParents, nodeAttr);

	if (parents != null) {
	    removeAdjacentNodes(parents, nodeAttr);
	}

	if (children != null) {
	    removeAdjacentNodes(children, nodeAttr);
	}

	if (matchChildren != null && matchChildren.isEmpty()) {
	    matchChildren = null;
	}

	if (matchParents != null && matchParents.isEmpty()) {
	    matchParents = null;
	}
    }

    /**
     * Remove nodes from the parents or children map
     * 
     * @param toClean
     *            Map to remove nodes
     * @param nodeAttr
     *            The node attribute of the node that should be removed
     */
    private void removeAdjacentNodes(Map<Long, String> toClean, String nodeAttr) {
	Iterator<Map.Entry<Long, String>> iter = toClean.entrySet().iterator();
	while (iter.hasNext()) {
	    if (iter.next().getValue().equals(nodeAttr)) {
		iter.remove();
	    }
	}
    }

    /**
     * Remove all {@link ResultValue}s from the given set with the according
     * attribute
     * 
     * @param matchSet
     *            Set where the results should be removed
     * @param id
     *            Id of the node
     */
    private void removeAttributeFromMatchSet(
	    Map<CompositeKeyNodeAttr, List<ResultValue>> matchSet,
	    String attribute) {
	if (matchSet != null) {
	    // Iterate through the result map
	    Iterator<CompositeKeyNodeAttr> iter = matchSet.keySet().iterator();
	    while (iter.hasNext()) {
		CompositeKeyNodeAttr entry = iter.next();

		if (attribute.equals(entry.getNodeAttribute())) {
		    iter.remove();
		}

	    }
	}
    }

    @Override
    public void addParentNode(Long id, String attribute) {
	if (parents == null) {
	    parents = new HashMap<>();
	}
	parents.put(id, attribute);
    }

    @Override
    public void addChildNode(Long id, String attribute) {
	if (children == null) {
	    children = new HashMap<>();
	}
	children.put(id, attribute);
    }

    @Override
    public VertexInterface deepCopy() {
	// this.matchChildren = null;
	// this.matchParents = null;

	Object obj = null;
	try {
	    // Write the object out to a byte array
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    ObjectOutputStream out = new ObjectOutputStream(bos);
	    out.writeObject(this);
	    out.flush();
	    out.close();

	    // Make an input stream from the byte array and read
	    // a copy of the object back in.
	    ObjectInputStream in = new ObjectInputStream(
		    new ByteArrayInputStream(bos.toByteArray()));
	    obj = in.readObject();
	} catch (IOException e) {
	    e.printStackTrace();
	} catch (ClassNotFoundException cnfe) {
	    cnfe.printStackTrace();
	}
	return (VertexInterface) obj;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result
		+ ((children == null) ? 0 : children.hashCode());
	result = prime * result
		+ ((matchChildren == null) ? 0 : matchChildren.hashCode());
	result = prime * result
		+ ((matchParents == null) ? 0 : matchParents.hashCode());
	result = prime * result
		+ ((matching == null) ? 0 : matching.hashCode());
	result = prime * result
		+ ((nodeAttribute == null) ? 0 : nodeAttribute.hashCode());
	result = prime * result + ((parents == null) ? 0 : parents.hashCode());
	result = prime * result + ((results == null) ? 0 : results.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (!(obj instanceof VertexAttribute))
	    return false;
	VertexAttribute other = (VertexAttribute) obj;
	if (children == null) {
	    if (other.children != null)
		return false;
	} else if (!children.equals(other.children))
	    return false;
	if (matchChildren == null) {
	    if (other.matchChildren != null)
		return false;
	} else if (!matchChildren.equals(other.matchChildren))
	    return false;
	if (matchParents == null) {
	    if (other.matchParents != null)
		return false;
	} else if (!matchParents.equals(other.matchParents))
	    return false;
	if (matching == null) {
	    if (other.matching != null)
		return false;
	} else if (!matching.equals(other.matching))
	    return false;
	if (nodeAttribute == null) {
	    if (other.nodeAttribute != null)
		return false;
	} else if (!nodeAttribute.equals(other.nodeAttribute))
	    return false;
	if (parents == null) {
	    if (other.parents != null)
		return false;
	} else if (!parents.equals(other.parents))
	    return false;
	if (results == null) {
	    if (other.results != null)
		return false;
	} else if (!results.equals(other.results))
	    return false;
	return true;
    }

    @Override
    public void updateNodeResults(
	    Map<CompositeKey, List<ResultValue>> nodeResults) {
	if (nodeResults != null) {

	    // Results not null => Update the results
	    this.results = nodeResults;
	    if (this.results.isEmpty()) {
		this.results = null;
		matching = false;
	    }
	}
    }
}
