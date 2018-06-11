package de.tf.uni.freiburg.sparkrdf.model.graph.edge;

import java.io.Serializable;

/**
 * Class that represents the edges of the graph. An edge could be a plain String
 * which is the case, if the edge attribute only occurs as predicate in the RDF
 * graph. If the edge attribute also exists as subject or object, than the Id of
 * this node is also stored inside this object.
 * 
 * @author Thorsten Berberich
 * 
 */
public class EdgeAttribute implements Serializable {

    /**
     * Generated serial UID
     */
    private static final long serialVersionUID = 4985112460850875214L;

    /**
     * Id of the node
     */
    private Long nodeId;

    /**
     * Attribute of the edge, is equal to the predicate of the RDF graph
     */
    private final String edgeAttribute;

    /**
     * Create a new edge, where the edge is also a node
     * 
     * @param nodeId
     *            ID of the node
     * @param edgeAttribute
     *            Attribute of the edge
     */
    public EdgeAttribute(Long nodeId, String edgeAttribute) {
	this.nodeId = nodeId;
	this.edgeAttribute = edgeAttribute;
    }

    /**
     * Create an edge where the edge isn't a node
     * 
     * @param edgeAttribute
     *            Attribute of the edge
     */
    public EdgeAttribute(String edgeAttribute) {
	this.edgeAttribute = edgeAttribute;
    }

    /**
     * Get the id of the node, if there's one
     * 
     * @return The id of the node
     */
    public Long getNodeId() {
	return nodeId;
    }

    /**
     * Get the attribute of this edge
     * 
     * @return The attribute
     */
    public String getEdgeAttribute() {
	return edgeAttribute;
    }

    /**
     * Check if there is an node Id
     * 
     * @return True if the node Id is present
     */
    public Boolean hasNodeId() {
	return nodeId != null;
    }

    @Override
    public String toString() {
	return "EdgeAttribute [nodeId=" + nodeId + ", edgeAttribute="
		+ edgeAttribute + "]";
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result
		+ ((edgeAttribute == null) ? 0 : edgeAttribute.hashCode());
	result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	EdgeAttribute other = (EdgeAttribute) obj;
	if (edgeAttribute == null) {
	    if (other.edgeAttribute != null)
		return false;
	} else if (!edgeAttribute.equals(other.edgeAttribute))
	    return false;
	if (nodeId == null) {
	    if (other.nodeId != null)
		return false;
	} else if (!nodeId.equals(other.nodeId))
	    return false;
	return true;
    }
}
