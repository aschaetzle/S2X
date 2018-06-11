package de.tf.uni.freiburg.sparkrdf.model.rdf.triple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.List;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.Position;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp.util.BGPVerifyUtil;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp.util.Shortcut;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.PartialResult;

/**
 * Class with utility functions to process {@link TriplePattern}s
 * 
 * @author Thorsten Berberich
 * 
 */
public final class TriplePatternUtils {

    /**
     * No public constructor because it's only a utility class
     */
    private TriplePatternUtils() {
    }

    /**
     * Check if the given {@link String} is a variable field or not.
     * 
     * @param toTest
     *            String to test
     * @return True iff the {@link String#startsWith(String)} "?"
     */
    public static Boolean isVariable(String toTest) {
	if (toTest.startsWith("?")) {
	    return true;
	}
	return false;
    }

    /**
     * Get the subject of a string representation from a triple pattern
     * 
     * @param strinRepresentation
     *            String representation from a triple pattern
     * @return The first part of the representation that is split by a tabulator
     */
    public static String getSubjectOfPresentation(String stringRepresentation) {
	return stringRepresentation.split("\t")[0];
    }

    /**
     * Get the predicate of a string representation from a triple pattern
     * 
     * @param stringRepresentation
     *            String representation from a triple pattern
     * @return The second part of the representation that is split by a
     *         tabulator
     */
    public static String getPredicateOfPresentation(String stringRepresentation) {
	return stringRepresentation.split("\t")[1];
    }

    /**
     * Get the object of a string representation from a triple pattern
     * 
     * @param stringRepresentation
     *            String representation from a triple pattern
     * @return The third part of the representation that is split by a tabulator
     */
    public static String getObjectOfPresentation(String stringRepresentation) {
	return stringRepresentation.split("\t")[2];
    }

    /**
     * Get all triple pattern which are all unique in the variable fields. This
     * means if two {@link TriplePattern} only differ in a constant field, one
     * will be ignored. For example: ?a foaf:knows ?b, ?a ex:likes ?b.
     * 
     * @param bgp
     *            All {@link TriplePattern}
     * @return Set of {@link TriplePattern} which are all unique in the variable
     *         field combinations
     */
    public static Set<TriplePattern> getTriplePatternsForResult(
	    List<TriplePattern> bgp2) {
	Set<TriplePattern> result = new HashSet<>();
	Set<String> allVariables = getAllVariablesOfBGP(bgp2);

	List<TriplePattern> bgp = new ArrayList<>(bgp2);
	Collections.sort(bgp, new VariableCountComparator());

	for (TriplePattern actual : bgp) {
	    Set<String> tpVars = getVariablesOfTriplePattern(actual);

	    if (allVariables.removeAll(tpVars)) {
		result.add(actual);

		// Get the shortcuts
		Set<Shortcut> shortcuts = BGPVerifyUtil.getShortcuts(bgp2,
			new CompositeKey(actual.getStringRepresentation(),
				Position.SUBJECT));
		if (!shortcuts.isEmpty()) {
		    // There are shortcuts of length three
		    for (Shortcut shortcut : shortcuts) {
			// TriplePattern with common join key
			TriplePattern tp1 = shortcut.getFirstTP()
				.getTriplePatternObject();
			if (allVariables
				.removeAll(getVariablesOfTriplePattern(tp1))) {
			    result.add(tp1);
			}
		    }
		}
	    }
	}
	return result;
    }

    /**
     * Get all variable fields of the basic graph pattern
     * 
     * @param bgp
     *            All {@link TriplePattern}s
     * @return Set containing all variable fields
     */
    private static Set<String> getAllVariablesOfBGP(
	    Collection<TriplePattern> bgp) {
	Set<String> res = new HashSet<>();

	for (TriplePattern tp : bgp) {
	    res.addAll(getVariablesOfTriplePattern(tp));
	}

	return res;
    }

    /**
     * Get all variable fields of a triple pattern.
     * 
     * @param tp
     *            {@link TriplePattern}
     * @return Set of all variable fields
     */
    public static Set<String> getVariablesOfTriplePattern(TriplePattern tp) {
	Set<String> result = new HashSet<>();
	if (isVariable(tp.getSubject())) {
	    result.add(tp.getSubject());
	}

	if (isVariable(tp.getPredicate())) {
	    result.add(tp.getPredicate());
	}

	if (isVariable(tp.getObject())) {
	    result.add(tp.getObject());
	}
	return result;
    }

    /**
     * Get all the {@link TriplePattern} that are used for the results which
     * contain all variable fields. They are split into {@link PartialResult},
     * which contain the same variables. The following example will be split
     * into two {@link PartialResult}s.
     * 
     * ?a foaf:knows ?b, ?x foaf:knows ?y
     * 
     * @param bgp2
     *            The basic graph pattern from
     *            {@link TriplePatternUtils#getTriplePatternsForResult(List)}
     * @return Set of the {@link PartialResult}s
     */
    public static List<PartialResult> getPartialResults(Set<TriplePattern> bgp2) {
	List<PartialResult> results = new ArrayList<>();
	List<TriplePattern> bgp = new ArrayList<>(bgp2);
	Collections.sort(bgp, new VariableCountComparator());

	// Check all triple pattern
	for (TriplePattern tp : bgp) {
	    Boolean added = false;
	    // Get the variables of the triple pattern
	    Set<String> actualVars = getVariablesOfTriplePattern(tp);

	    // Check if it can be added to a partial result
	    for (PartialResult result : results) {
		Set<String> variables = getAllVariablesOfBGP(result
			.getAllTriples());

		// Calculate the intersection
		variables.retainAll(actualVars);

		// Intersection not empty
		if (!variables.isEmpty()) {
		    /*
		     * Intersection not empty, add the triple pattern to this
		     * partial result
		     */
		    added = true;
		    result.addTriplePattern(tp);
		    break;
		}
	    }

	    if (!added) {
		// Add a new partial result
		results.add(new PartialResult(tp));
	    }

	    Set<PartialResult> toDelete = new HashSet<>();

	    // Check if partial results could be merged
	    for (PartialResult result : results) {

		for (PartialResult res2 : results) {

		    Set<String> variables1 = getAllVariablesOfBGP(result
			    .getAllTriples());

		    if (!result.equals(res2)) {
			// Check if they have common variables
			Set<String> variables2 = getAllVariablesOfBGP(res2
				.getAllTriples());

			variables1.retainAll(variables2);
			if (!variables1.isEmpty()) {

			    // Merge results
			    result.addAllTriplePattern(res2.getAllTriples());
			    res2.clear();
			    toDelete.add(res2);
			}
		    }
		}
	    }

	    // Remove unused results
	    results.removeAll(toDelete);
	}
	return results;
    }

    /**
     * Get the partial results with sorted TriplePatterns
     * 
     * @param bgp
     *            The basic graph pattern from
     *            {@link TriplePatternUtils#getTriplePatternsForResult(List)}
     * @return Sorted {@link PartialResult}s
     * @see {@link TriplePatternUtils#sortPartialResults(Set)}
     *      {@link TriplePatternUtils#getPartialResults(Set))}
     */
    public static List<PartialResult> getSortedPartialResults(
	    Set<TriplePattern> bgp) {
	List<PartialResult> results = getPartialResults(bgp);
	sortPartialResults(results);
	return results;
    }

    /**
     * Sorts the {@link TriplePattern} inside the given {@link PartialResult}s
     * in that order that every {@link TriplePattern} has a common variable with
     * the next one.
     */
    public static void sortPartialResults(List<PartialResult> results) {
	for (PartialResult result : results) {
	    List<TriplePattern> sorted = new ArrayList<>();

	    Queue<TriplePattern> working = new LinkedList<>();
	    working.addAll(result.getAllTriples());

	    while (!working.isEmpty()) {
		TriplePattern next = working.poll();

		// Add the first TriplePattern
		if (sorted.isEmpty()) {
		    sorted.add(next);
		    continue;
		}

		Set<String> workingVars = getAllVariablesOfBGP(sorted);
		Set<String> tpVars = getVariablesOfTriplePattern(next);

		tpVars.retainAll(workingVars);
		if (tpVars.isEmpty()) {
		    // No intersection, add to the back of the queue
		    working.add(next);
		} else {
		    sorted.add(next);
		}
	    }
	    result.replaceTriplePattern(sorted);
	}
    }

    /**
     * Get the common variable fields between the set of variables and the given
     * {@link TriplePattern}
     * 
     * @param variables
     *            Set of variable fields
     * @param tp2
     *            Second {@link TriplePattern}
     * @return List of common variable fields
     */
    public static List<String> getJoinVariables(Set<String> variables,
	    TriplePattern tp2) {
	List<String> result = new ArrayList<>();

	if (isVariable(tp2.getSubject())
		&& variables.contains(tp2.getSubject())) {
	    if (!result.contains(tp2.getSubject())) {
		result.add(tp2.getSubject());
	    }
	}

	if (isVariable(tp2.getPredicate())
		&& variables.contains(tp2.getPredicate())) {
	    if (!result.contains(tp2.getPredicate())) {
		result.add(tp2.getPredicate());
	    }
	}

	if (isVariable(tp2.getObject()) && variables.contains(tp2.getObject())) {
	    if (!result.contains(tp2.getObject())) {
		result.add(tp2.getObject());
	    }
	}

	return result;
    }

    /**
     * Check if the given object only occurs as object and is therefore a
     * literal and it should only occur once as a object. Then this object
     * hasn't to be stored in the object node.F
     * 
     * @param bgp
     *            List of {@link TriplePattern}
     * @param object
     *            Object to check
     * @return True if it is a literal and mustn't be stored in the object node
     */
    public static Boolean isObjectLiteral(List<TriplePattern> bgp, String object) {
	Set<String> subPred = new HashSet<>();
	int objCount = 0;
	for (TriplePattern tp : bgp) {
	    subPred.add(tp.getSubject());
	    subPred.add(tp.getPredicate());
	    if (tp.getObject().equals(object)) {
		objCount++;
	    }
	}
	return !subPred.contains(object) && objCount == 1;
    }
}
