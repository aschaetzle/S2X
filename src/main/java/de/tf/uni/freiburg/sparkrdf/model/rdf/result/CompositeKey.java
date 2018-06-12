package de.tf.uni.freiburg.sparkrdf.model.rdf.result;

import java.io.Serializable;

import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePatternUtils;

/**
 * Key that is used to store the results for a node
 * 
 * @author Thorsten Berberich
 * 
 */
public class CompositeKey implements Serializable {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -7160486741128000300L;

    /**
     * The triple pattern of the result. see
     * {@link TriplePattern#getStringRepresentation()}
     */
    private final String triplePattern;

    /**
     * {@link Position} the node is in the result
     */
    private final Position pos;

    /**
     * Create a new key for a result.
     * 
     * @param triplePattern
     *            String representation of the triple pattern. See
     *            {@link TriplePattern#getStringRepresentation()}
     * @param Position
     *            {@link Position} the node is in the result
     */
    public CompositeKey(String triplePattern, Position position) {
	this.triplePattern = triplePattern.intern();
	this.pos = position;
    }

    /**
     * Check if the node was the subject
     * 
     * @return True if it is the subject
     */
    public Boolean isSubject() {
	return pos.equals(Position.SUBJECT);
    }

    /**
     * Check if the node was the object
     * 
     * @return True if it is the object
     */
    public Boolean isObject() {
	return pos.equals(Position.OBJECT);
    }

    /**
     * Check if the node was the predicate
     * 
     * @return True if it is the predicate
     */
    public Boolean isPredicate() {
	return pos.equals(Position.PREDICATE);
    }

    /**
     * Get the triple pattern of the result
     * 
     * @return The triple pattern as {@link String}
     */
    public String getTriplePattern() {
	return triplePattern;
    }

    /**
     * Get the {@link TriplePattern}
     * 
     * @return {@link TriplePattern}
     */
    public TriplePattern getTriplePatternObject() {
	String[] split = triplePattern.split("\t");
	return new TriplePattern(split[0], split[1], split[2]);
    }

    /**
     * Get the field which the node is
     * 
     * @return Subject, object or predicate of the triple pattern
     */
    public String getField() {
	switch (pos) {
	case SUBJECT:
	    return TriplePatternUtils
		    .getSubjectOfPresentation(this.triplePattern);
	case PREDICATE:
	    return TriplePatternUtils
		    .getPredicateOfPresentation(this.triplePattern);
	case OBJECT:
	    return TriplePatternUtils
		    .getObjectOfPresentation(this.triplePattern);
	default:
	    // Could not happen
	    return null;
	}
    }

    /**
     * Get the subject of this triple pattern
     * 
     * @return Subject of the triple pattern
     */
    public String getSubject() {
	return TriplePatternUtils.getSubjectOfPresentation(this.triplePattern);
    }

    /**
     * Get the predicate of this triple pattern
     * 
     * @return Predicate of the triple pattern
     */
    public String getPredicate() {
	return TriplePatternUtils
		.getPredicateOfPresentation(this.triplePattern);
    }

    /**
     * Get the object of this triple pattern
     * 
     * @return Object of the triple pattern
     */
    public String getObject() {
	return TriplePatternUtils.getObjectOfPresentation(this.triplePattern);
    }

    /**
     * Get the position of the field of the node
     * 
     * @return {@link Position}
     */
    public Position getPosition() {
	return pos;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((pos == null) ? 0 : pos.hashCode());
	result = prime * result
		+ ((triplePattern == null) ? 0 : triplePattern.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (!(obj instanceof CompositeKey))
	    return false;
	CompositeKey other = (CompositeKey) obj;
	if (pos != other.pos)
	    return false;
	if (triplePattern == null) {
	    if (other.triplePattern != null)
		return false;
	} else if (!triplePattern.equals(other.triplePattern))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return "Key [triplePattern=" + triplePattern + ", pos=" + pos + "]";
    }

}
