package de.tf.uni.freiburg.sparkrdf.model.rdf.result;

import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;

/**
 * Class that extends the {@link CompositeKey} to also store the attribute of a
 * node. This class is used to send the values of a node to his parent and child
 * nodes.
 * 
 * @author Thorsten Berberich
 * 
 */
public class CompositeKeyNodeAttr extends CompositeKey {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = 1447265014439937491L;

    /**
     * Attribute of the node
     */
    private final String nodeAttribute;

    /**
     * Create a new {@link CompositeKey} with node attribute
     * 
     * @param triplePattern
     *            see {@link TriplePattern#getStringRepresentation()}
     * @param position
     *            Position which the node is in the result
     * @param nodeAttribute
     *            Attribute of the node
     */
    public CompositeKeyNodeAttr(String triplePattern, Position position,
	    String nodeAttribute) {
	super(triplePattern, position);
	this.nodeAttribute = nodeAttribute;
    }

    /**
     * Create a new {@link CompositeKey} with node attribute.
     * 
     * @param key
     *            The key to add the attribute
     * @param nodeAttribute
     *            Attribute of the node
     */
    public CompositeKeyNodeAttr(CompositeKey key, String nodeAttribute) {
	super(key.getTriplePattern(), key.getPosition());
	this.nodeAttribute = nodeAttribute.intern();
    }

    /**
     * Get the node attribute
     * 
     * @return NodeTest attribute
     */
    public String getNodeAttribute() {
	return nodeAttribute;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = super.hashCode();
	result = prime * result
		+ ((nodeAttribute == null) ? 0 : nodeAttribute.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (!super.equals(obj))
	    return false;
	if (!(obj instanceof CompositeKeyNodeAttr))
	    return false;
	CompositeKeyNodeAttr other = (CompositeKeyNodeAttr) obj;
	if (nodeAttribute == null) {
	    if (other.nodeAttribute != null)
		return false;
	} else if (!nodeAttribute.equals(other.nodeAttribute))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return "CompositeKeyNodeAttr [" + super.toString() + " nodeAttribute= "
		+ nodeAttribute + "]";
    }

}
