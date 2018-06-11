package de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp.util;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;

/**
 * Represents a shortcut of the following form, e.g. ?a knows ?b, ?b knows ?c,
 * ?a knows ?c
 * 
 * @author Thorsten Berberich
 * 
 */
public class Shortcut {

    /**
     * First triple pattern that must be fulfilled
     */
    private final CompositeKey tp1;

    /**
     * Second triple pattern that must be fulfilled
     */
    private final CompositeKey tp2;

    /**
     * Create a new shortcut for a given key, the other two
     * {@link TriplePattern} are stored in this object as {@link CompositeKey}s
     * 
     * @param tp1
     *            First triple pattern that must be fulfilled
     * @param tp2
     *            Second triple pattern that must be fulfilled
     */
    public Shortcut(CompositeKey tp1, CompositeKey tp2) {
	this.tp1 = tp1;
	this.tp2 = tp2;
    }

    /**
     * Get the first {@link TriplePattern} that must be fulfilled.
     * 
     * @return First {@link CompositeKey}
     */
    public CompositeKey getFirstTP() {
	return tp1;
    }

    /**
     * Get the second {@link TriplePattern} that must be fulfilled.
     * 
     * @return Second {@link CompositeKey}
     */
    public CompositeKey getSecondtTP() {
	return tp2;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((tp1 == null) ? 0 : tp1.hashCode());
	result = prime * result + ((tp2 == null) ? 0 : tp2.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (!(obj instanceof Shortcut))
	    return false;
	Shortcut other = (Shortcut) obj;
	if (tp1 == null) {
	    if (other.tp1 != null)
		return false;
	} else if (!tp1.equals(other.tp1))
	    return false;
	if (tp2 == null) {
	    if (other.tp2 != null)
		return false;
	} else if (!tp2.equals(other.tp2))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return "Shortcut [tp1=" + tp1 + ", tp2=" + tp2 + "]";
    }

}
