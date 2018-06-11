package de.tf.uni.freiburg.sparkrdf.sparql.message.datatype;

/**
 * Class to store a parent or child node relationship for another node
 * 
 * @author Thorsten Berberich
 * 
 */
public class AdjacentNode {

    /**
     * Id of the child or parent node
     */
    private final long Id;

    /**
     * Attribute of the child or parent node
     */
    private final String attribute;

    /**
     * True if this node is the parent of the other node
     */
    private final Boolean isParent;

    /**
     * Creates a new object to store a relationship between two nodes
     * 
     * @param Id
     *            NodeTest Id of the child or parent node
     * @param attribute
     *            Attribute of the child or parent node
     * @param isParent
     *            True iff the node is the parent of the other node, false if it
     *            is the child
     */
    public AdjacentNode(long Id, String attribute, Boolean isParent) {
	this.Id = Id;
	this.attribute = attribute;
	this.isParent = isParent;
    }

    /**
     * Get the node Id of the parent or child node
     * 
     * @return NodeTest Id
     */
    public long getId() {
	return Id;
    }

    /**
     * Get the node attribute of the parent or child node
     * 
     * @return NodeTest attribute
     */
    public String getAttribute() {
	return attribute;
    }

    /**
     * Check if the node is the parent or the child
     * 
     * @return True if it is the parent of the node, false if it is the child
     */
    public Boolean isParent() {
	return isParent;
    }

    @Override
    public String toString() {
	return "AdjacentNode [Id=" + Id + ", attribute=" + attribute
		+ ", isParent=" + isParent + "]";
    }

}
