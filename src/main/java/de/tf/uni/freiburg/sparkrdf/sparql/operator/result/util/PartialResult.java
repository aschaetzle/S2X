package de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util;

import java.util.ArrayList;
import java.util.List;

import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;

/**
 * Represents a part of the result. This means that the sum of all
 * {@link TriplePattern}s contained in all {@link PartialResult}s make up the
 * whole result. A clustering in more than one partial results happen for
 * example with this basic graph pattern:
 * 
 * ?a knows ?b, ?y knows ?z
 * 
 * @author Thorsten Berberich
 * 
 */
public class PartialResult {

    /**
     * All {@link TriplePattern}s of this result
     */
    private List<TriplePattern> triples;

    /**
     * Create a new partial result
     */
    public PartialResult() {
	triples = new ArrayList<>();
    }

    /**
     * Create a new partial result with the given {@link TriplePattern}
     * 
     * @param tp
     *            First {@link TriplePattern}
     */
    public PartialResult(TriplePattern tp) {
	triples = new ArrayList<>();
	triples.add(tp);
    }

    /**
     * Add a {@link TriplePattern} to this partial result
     * 
     * @param tp
     *            {@link TriplePattern}
     */
    public void addTriplePattern(TriplePattern tp) {
	this.triples.add(tp);
    }

    /**
     * Adds all {@link TriplePattern}s to this partial result
     * 
     * @param tps
     *            Set of {@link TriplePattern}
     */
    public void addAllTriplePattern(List<TriplePattern> tps) {
	this.triples.addAll(tps);
    }

    /**
     * Replace all {@link TriplePattern} by the given one
     * 
     * @param tps
     *            New {@link TriplePattern}
     */
    public void replaceTriplePattern(List<TriplePattern> tps) {
	this.triples.clear();
	triples.addAll(tps);
    }

    /**
     * Get all {@link TriplePattern} of this partial result
     * 
     * @return List of {@link TriplePattern}
     */
    public List<TriplePattern> getAllTriples() {
	return triples;
    }

    /**
     * Clear the set of triples
     */
    public void clear() {
	triples.clear();
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((triples == null) ? 0 : triples.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (!(obj instanceof PartialResult))
	    return false;
	PartialResult other = (PartialResult) obj;
	if (triples == null) {
	    if (other.triples != null)
		return false;
	} else if (!triples.equals(other.triples))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	return "PartialResult [triples=" + triples + "]";
    }
}
