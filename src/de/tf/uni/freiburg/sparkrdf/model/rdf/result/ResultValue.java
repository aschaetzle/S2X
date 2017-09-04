package de.tf.uni.freiburg.sparkrdf.model.rdf.result;

import java.io.Serializable;

/**
 * Class where a result of one node is stored in
 * 
 * @author Thorsten Berberich
 * 
 */
public class ResultValue implements Serializable {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -1526575184382911495L;

    /**
     * Attribute of the edge
     */
    private final String edgeAttribute;

    /**
     * The attribute of the connected node
     */
    private final String connectedNodeAttibute;

    /**
     * Creates a new result for a node
     * 
     * @param edgeAttribute
     *            The attribute of the edge
     * @param connectedNodeAttibute
     *            The attribute of the node this node is connected to
     */
    public ResultValue(String edgeAttribute, String connectedNodeAttibute) {
	this.edgeAttribute = edgeAttribute.intern();
	this.connectedNodeAttibute = connectedNodeAttibute.intern();
    }

    /**
     * Get the attribute of the edge
     * 
     * @return Edge attribute
     */
    public String getEdgeAttribute() {
	return edgeAttribute.intern();
    }

    /**
     * Get the attribute of the connected node
     * 
     * @return Attribute of the connected node
     */
    public String getConnectedNodeAttribute() {
	return connectedNodeAttibute;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime
		* result
		+ ((connectedNodeAttibute == null) ? 0 : connectedNodeAttibute
			.hashCode());
	result = prime * result
		+ ((edgeAttribute == null) ? 0 : edgeAttribute.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (!(obj instanceof ResultValue))
	    return false;
	ResultValue other = (ResultValue) obj;
	if (connectedNodeAttibute == null) {
	    if (other.connectedNodeAttibute != null)
		return false;
	} else if (!connectedNodeAttibute.equals(other.connectedNodeAttibute))
	    return false;
	if (edgeAttribute == null) {
	    if (other.edgeAttribute != null)
		return false;
	} else if (!edgeAttribute.equals(other.edgeAttribute))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return "ResultValue [edgeAttribute=" + edgeAttribute
		+ ", connectedNodeAttibute=" + connectedNodeAttibute + "]";
    }
}
