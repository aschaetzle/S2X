package de.tf.uni.freiburg.sparkrdf.model.rdf.triple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import de.tf.uni.freiburg.sparkrdf.model.graph.node.VertexAttribute;
import de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.IExpression;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

public class TriplePattern implements Serializable {

    /**
     * Generated serial ID
     */
    private static final long serialVersionUID = -5830558469298129077L;

    /**
     * Subject of this query triple, could also be a variable starting with ?
     */
    private final String subject;

    /**
     * Object of this query triple, could also be a variable starting with ?
     */
    private final String object;

    /**
     * Predicate of this query triple, could also be a variable starting with ?
     */
    private final String predicate;

    /**
     * String representation that is stored inside the results
     */
    private final String representation;

    /**
     * Filter expressions for this {@link TriplePattern}
     */
    private List<IExpression> expressions = null;

    /**
     * Creates a new query triple
     * 
     * @param subject
     *            Subject of this query triple, could also be a variable
     *            starting with ?
     * @param pred
     *            Predicate of this query triple, could also be a variable
     *            starting with ?
     * @param object
     *            Object of this query triple, could also be a variable starting
     *            with ?
     */
    public TriplePattern(String subject, String pred, String object) {
	this.subject = subject;
	this.predicate = pred;
	this.object = object;
	this.representation = subject + "\t" + predicate + "\t"
		+ object.intern();
    }

    /**
     * Checks a part of a triplet against the according part of the Sparql
     * triple
     * 
     * @param toCheck
     *            Part of the triplet to check
     * @param toFulfill
     *            Part of the Sparql triple to be fulfilled
     * @return True iff toCheck fulfills the part of the Sparql triple
     */
    private Boolean checkQueryPart(String toCheck, String toFulfill) {
	if (TriplePatternUtils.isVariable(toFulfill)) {
	    // A variable is fulfilled by every field
	    return true;
	} else {
	    // The two parts have to match
	    return toCheck.equals(toFulfill);
	}
    }

    /**
     * Get the object of this query triple
     * 
     * @return the object
     */
    public String getObject() {
	return object;
    }

    /**
     * Get the predicate of this query triple
     * 
     * @return the predicate
     */
    public String getPredicate() {
	return predicate;
    }

    /**
     * Get the subject of this query triple
     * 
     * @return the subject
     */
    public String getSubject() {
	return subject;
    }

    /**
     * Check if the predicate is a variable field
     * 
     * @return True iff the predicate is a variable field
     */
    public Boolean isPredicateVariable() {
	return TriplePatternUtils.isVariable(predicate);
    }

    /**
     * Check if the attribute is somewhere in this triple pattern
     * 
     * @param attribute
     *            Attribute to search
     * @return True iff the attribute is at least a subject or object or
     *         predicate
     */
    public Boolean contains(String attribute) {
	if (subject.equals(attribute)) {
	    return true;
	}

	if (predicate.equals(attribute)) {
	    return true;
	}

	if (object.equals(attribute)) {
	    return true;
	}

	return false;
    }

    /**
     * Check if a graph triplet fulfills this Sparql triple.
     * 
     * @param subject
     *            Subject of the triplet
     * @param predicate
     *            Predicate of the triplet
     * @param object
     *            Object of the triplet
     * @return true iff subject, predicate and object fulfill this triple
     */
    public Boolean isFulfilledByTriplet(VertexAttribute subject,
	    String predicate, VertexAttribute object) {
	final Boolean sub = checkQueryPart(subject.getAttribute(), this.subject);
	final Boolean pred = checkQueryPart(predicate, this.predicate);
	final Boolean obj = checkQueryPart(object.getAttribute(), this.object);

	/*
	 * Check if the subject or object of this triple pattern is equal to the
	 * predicate, if yes then the subject or object should be equal to the
	 * predicate
	 */
	if (!isEqualToPredicate(this.subject, subject, predicate)) {
	    return false;
	}

	if (!isEqualToPredicate(this.object, object, predicate)) {
	    return false;
	}

	/*
	 * Check if the subject and object of this triple pattern are equal, if
	 * this is true then the subject and object node of the RDF triplet
	 * should be equal too.
	 */
	if (!compareSubjectAndObject(subject, object)) {
	    return false;
	}

	/*
	 * Check the filter expressions if there are some for this triple
	 * pattern
	 */
	if (expressions != null) {
	    Boolean result = true;

	    SolutionMapping mapping = new SolutionMapping(
		    this.getStringRepresentation());
	    mapping.addMapping(this.subject, subject.getAttribute());
	    mapping.addMapping(this.predicate, predicate);
	    mapping.addMapping(this.object, object.getAttribute());

	    for (IExpression expr : expressions) {
		result = result && expr.evaluate(mapping);
	    }

	    if (!result) {
		return false;
	    }
	}

	return sub && pred && obj;
    }

    /**
     * Check if the subject or object of this triple pattern is equal to the
     * predicate. If yes then check that the given node equals the given
     * predicate
     * 
     * @param toCheck
     *            Subject or object of this triple pattern
     * 
     * @param node
     *            NodeTest that should be equal to the predicate
     * 
     * @param predicate
     *            Predicate of the RDF triple
     * 
     * @return True if the node and the predicate are equal if this is also the
     *         case in the triple pattern, or the predicate and the given
     *         subject or object are not equal
     */
    private Boolean isEqualToPredicate(String toCheck, VertexAttribute node,
	    String predicate) {
	if (toCheck.equals(this.predicate)) {
	    return node.getAttribute().equals(predicate);
	} else {
	    return true;
	}
    }

    /**
     * Check if the subject and object of this triple are equal. If yes then the
     * two given nodes should be equal too.
     * 
     * @param subject
     *            NodeTest subject
     * @param object
     *            NodeTest object
     * @return True iff the nodes are equal and the subject and object are equal
     *         of this triple pattern, or the subject and object of this triple
     *         pattern are unequal
     */
    private Boolean compareSubjectAndObject(VertexAttribute subject,
	    VertexAttribute object) {
	if (this.subject.equals(this.object)) {
	    // Should be the same node
	    return subject.getAttribute().equals(object.getAttribute());
	} else {
	    return true;
	}
    }

    /**
     * Add a filter expressions to this triple pattern
     * 
     * @param expr
     *            Filter {@link IExpression}
     */
    public void addFilterExpression(IExpression expr) {
	if (expressions == null) {
	    expressions = new ArrayList<IExpression>();
	}
	this.expressions.add(expr);
    }

    /**
     * Get the string representation that is used to store results
     * 
     * @return String with Subject\tPredicate\tObject
     */
    public String getStringRepresentation() {
	return representation;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((object == null) ? 0 : object.hashCode());
	result = prime * result
		+ ((predicate == null) ? 0 : predicate.hashCode());
	result = prime * result
		+ ((representation == null) ? 0 : representation.hashCode());
	result = prime * result + ((subject == null) ? 0 : subject.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (!(obj instanceof TriplePattern))
	    return false;
	TriplePattern other = (TriplePattern) obj;
	if (object == null) {
	    if (other.object != null)
		return false;
	} else if (!object.equals(other.object))
	    return false;
	if (predicate == null) {
	    if (other.predicate != null)
		return false;
	} else if (!predicate.equals(other.predicate))
	    return false;
	if (representation == null) {
	    if (other.representation != null)
		return false;
	} else if (!representation.equals(other.representation))
	    return false;
	if (subject == null) {
	    if (other.subject != null)
		return false;
	} else if (!subject.equals(other.subject))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	final String res = "Triple Pattern\nSubject: " + subject + "\t"
		+ predicate + "\t" + object + "\n";
	return res;
    }
}
