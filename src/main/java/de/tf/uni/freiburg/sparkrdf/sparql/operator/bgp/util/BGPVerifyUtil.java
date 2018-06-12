package de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.Position;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePatternUtils;

/**
 * A utility class that can be used to verify the results of a node
 * 
 * @author Thorsten Berberich
 * 
 */
public final class BGPVerifyUtil {

    /**
     * Private constructor. No objects of this class needed.
     */
    private BGPVerifyUtil() {
    }

    /**
     * Check that all triple pattern for a specific field are existing
     * 
     * @param key
     *            The {@link CompositeKey} of the result, this gives the field
     *            to check
     * @param results
     *            All results of one node
     * @param bgp
     *            All {@link TriplePattern}
     * @return True if all triple pattern for the given field where found in the
     *         results
     */
    public static Boolean checkTriplePatternGroup(CompositeKey key,
	    Map<CompositeKey, List<ResultValue>> results,
	    List<TriplePattern> bgp) {

	if (results == null) {
	    return false;
	}

	// All triple pattern that must be fulfilled
	Set<CompositeKey> conditions = getDependingCompositeKeys(key, bgp, true);

	if (!conditions.isEmpty()) {
	    for (CompositeKey condition : conditions) {
		List<ResultValue> result = results.get(condition);

		if (result == null || result.isEmpty()) {
		    /*
		     * Result for triple pattern is not present or the results
		     * are empty
		     */
		    return false;
		}
	    }
	}

	return true;
    }

    /**
     * Check that the adjacent connections are available in the parent and child
     * nodes. If a node is the subject of the result then there must be an
     * according {@link ResultValue} in the parent results, with the parent node
     * as object. <br>
     * If a node is an object then there must be an according
     * {@link ResultValue} in the child results where this node is the subject.<br>
     * If the predicate is a variable field, then there must be a
     * {@link ResultValue} either in the child or parent matches. If a node is
     * at the predicate position, then there must be the according result in the
     * parent and child results.
     * 
     * @param key
     *            {@link CompositeKey} which denotes the actual triple pattern
     * @param nodeResults
     *            All results of the node
     * @param parentMatches
     *            All results of the parent nodes
     * @param childMatches
     *            All results of the child nodes
     * @return True if all according result were found.
     */
    public static Boolean checkAdjacentConnections(CompositeKey key,
	    Map<CompositeKey, List<ResultValue>> nodeResults,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> parentMatches,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> childMatches,
	    List<TriplePattern> bgp, String nodeAttribute) {
	Boolean changed = false;

	List<ResultValue> fieldResults = nodeResults.get(key);

	// NodeTest results iterator for the triple pattern
	Iterator<ResultValue> itr = fieldResults.iterator();

	// Iterate through all results and search the connections
	while (itr.hasNext()) {
	    ResultValue actual = itr.next();

	    if (key.isSubject()) {
		if (!checkAdjacentConnections(key, actual, childMatches, bgp,
			nodeAttribute)) {
		    // According result not found
		    itr.remove();
		    changed = true;
		    continue;
		}

	    } else if (key.isObject()) {
		// Only search in the parent results
		if (!checkAdjacentConnections(key, actual, parentMatches, bgp,
			nodeAttribute)) {
		    // According result not found
		    itr.remove();
		    changed = true;
		    continue;
		}
	    } else if (key.isPredicate()) {
		// Search in both, parent and child results
		if (!checkPredicateConnections(key, actual, parentMatches,
			childMatches, nodeAttribute)) {
		    // According result not found
		    itr.remove();
		    changed = true;
		    continue;
		}
	    }
	}

	if (fieldResults.isEmpty()) {
	    // No results left for this triple pattern, remove it
	    nodeResults.remove(key);
	}
	return !changed;
    }

    /**
     * Check if the according results are existing in the child and parent
     * results for the variable predicate field.
     * 
     * @param key
     *            Actual {@link CompositeKey}
     * @param actual
     *            Actual {@link ResultValue}
     * @param parentMatches
     *            All {@link ResultValue}s of the parent nodes
     * @param childMatches
     *            All {@link ResultValue}s of the child nodes
     * @return True if the according results were found in the child and parent
     *         nodes
     */
    private static Boolean checkPredicateConnections(CompositeKey key,
	    ResultValue actual,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> parentMatches,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> childMatches,
	    String nodeAttribute) {

	// Check the direction to the parent node
	CompositeKeyNodeAttr subjectKey = new CompositeKeyNodeAttr(
		key.getTriplePattern(), Position.SUBJECT,
		actual.getEdgeAttribute());
	List<ResultValue> subjectResults = parentMatches.get(subjectKey);

	if (subjectResults == null || subjectResults.isEmpty()) {
	    return false;
	}

	ResultValue findSubject = new ResultValue(nodeAttribute,
		actual.getConnectedNodeAttribute());

	// Subject not found in parent results
	if (!subjectResults.contains(findSubject)) {
	    return false;
	}

	// Check the child results
	CompositeKeyNodeAttr objectKey = new CompositeKeyNodeAttr(
		key.getTriplePattern(), Position.OBJECT,
		actual.getConnectedNodeAttribute());
	List<ResultValue> objectResults = childMatches.get(objectKey);

	if (objectResults == null || objectResults.isEmpty()) {
	    return false;
	}

	// Result with switched connected and node attribute
	ResultValue findObject = new ResultValue(nodeAttribute,
		actual.getEdgeAttribute());

	// Object not found in child results
	if (!objectResults.contains(findObject)) {
	    return false;
	}

	return true;
    }

    public static Boolean checkTriplePatternInstantiation(CompositeKey key,
	    Map<CompositeKey, List<ResultValue>> nodeResults,
	    List<TriplePattern> bgp, String nodeAttribute) {
	Boolean changed = false;

	Set<CompositeKey> joinKeys = getJoinCompositeKeys(key, bgp);

	if (joinKeys.isEmpty()) {
	    // No join keys to check for this triple pattern
	    return true;
	}

	List<ResultValue> toCheck = nodeResults.get(key);

	if (toCheck == null || toCheck.isEmpty()) {
	    return false;
	}

	HashMap<String, String> mapping = new HashMap<>();

	// Check every result for that key
	Iterator<ResultValue> itr = toCheck.iterator();
	while (itr.hasNext()) {
	    mapping.clear();
	    ResultValue actualResult = itr.next();

	    /*
	     * Put the values into the mapping with the variable fields
	     */
	    if (key.isSubject()) {
		mapping.put(key.getSubject(), nodeAttribute);
		mapping.put(key.getObject(),
			actualResult.getConnectedNodeAttribute());
		mapping.put(key.getPredicate(), actualResult.getEdgeAttribute());
	    } else if (key.isObject()) {
		mapping.put(key.getSubject(),
			actualResult.getConnectedNodeAttribute());
		mapping.put(key.getObject(), nodeAttribute);
		mapping.put(key.getPredicate(), actualResult.getEdgeAttribute());
	    } else if (key.isPredicate()) {
		mapping.put(key.getSubject(), actualResult.getEdgeAttribute());
		mapping.put(key.getObject(),
			actualResult.getConnectedNodeAttribute());
	    }

	    // Check all join keys for this result
	    for (CompositeKey joinKey : joinKeys) {
		List<ResultValue> joinResults = nodeResults.get(joinKey);

		if (joinResults == null || joinResults.isEmpty()) {
		    itr.remove();
		    changed = true;
		    break;
		}

		Boolean found = false;

		String pred;
		String obj;

		for (ResultValue join : joinResults) {
		    if (joinKey.isSubject() || joinKey.isPredicate()) {
			pred = replaceFieldByValue(mapping,
				joinKey.getPredicate());
			obj = replaceFieldByValue(mapping, joinKey.getObject());

		    } else {
			// Key is object
			obj = replaceFieldByValue(mapping, joinKey.getSubject());
			pred = replaceFieldByValue(mapping,
				joinKey.getPredicate());
		    }

		    if (join.getConnectedNodeAttribute().equals(obj)) {
			if (TriplePatternUtils.isVariable(pred)) {
			    found = true;
			    break;
			} else if (join.getEdgeAttribute().equals(pred)) {
			    found = true;
			    break;
			}
		    }
		}

		// Check if the result is found or not
		if (!found) {
		    itr.remove();
		    changed = true;
		    break;
		}
	    }
	}

	if (toCheck.isEmpty()) {
	    // No results left, delete the key
	    nodeResults.remove(key);
	}

	return !changed;
    }

    /**
     * Replaces the given field by its value if found in the mapping.
     * 
     * @param mapping
     *            Mapping from variable fields to values
     * @param field
     *            Field to replace by the value
     * @return The value if found in the mapping, else the original field
     */
    private static String replaceFieldByValue(Map<String, String> mapping,
	    String field) {
	String result = mapping.get(field);
	return (result != null) ? result : field;
    }

    /**
     * Check if there is the according result in the adjacentF results
     * 
     * @param key
     *            Actual {@link CompositeKey}
     * @param actual
     *            Actual {@link ResultValue}
     * @param childMatches
     *            All results of all children
     * @param nodeAttribute
     *            Attribute of the node
     * @return True if the according result was found and nothing was changed
     */
    private static Boolean checkAdjacentConnections(CompositeKey key,
	    ResultValue actual,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> toSearchIn,
	    List<TriplePattern> bgp, String nodeAttribute) {
	Position otherFieldPos = null;

	if (key.isSubject()) {
	    otherFieldPos = Position.OBJECT;
	} else if (key.isObject()) {
	    otherFieldPos = Position.SUBJECT;
	}

	// Don't search for an adjacent connection if the object is a literal
	if (!TriplePatternUtils.isObjectLiteral(bgp, key.getObject())) {
	    if (toSearchIn == null) {
		// No adjacent results
		return false;
	    }

	    // Only search in the child or parent results
	    CompositeKeyNodeAttr findResults = new CompositeKeyNodeAttr(
		    key.getTriplePattern(), otherFieldPos,
		    actual.getConnectedNodeAttribute());
	    List<ResultValue> adjacentResults = toSearchIn.get(findResults);

	    // Results not existing
	    if (adjacentResults == null || adjacentResults.isEmpty()) {
		return false;
	    }

	    // Search according result in adjacent results
	    ResultValue toFind = new ResultValue(actual.getEdgeAttribute(),
		    nodeAttribute);

	    if (!adjacentResults.contains(toFind)) {
		return false;
	    }
	}

	/*
	 * Check if the predicate is a variable field, but only if the predicate
	 * occurs in another triple pattern as object or subject
	 */
	if (TriplePatternUtils.isVariable(key.getPredicate())
		&& isSubjectOrObject(
			TriplePatternUtils.getPredicateOfPresentation(key
				.getTriplePattern()), bgp)) {

	    CompositeKeyNodeAttr findPredicateRes = new CompositeKeyNodeAttr(
		    key.getTriplePattern(), Position.PREDICATE,
		    actual.getEdgeAttribute());
	    List<ResultValue> predicateResults = toSearchIn
		    .get(findPredicateRes);

	    // Predicate result not found
	    if (predicateResults == null || predicateResults.isEmpty()) {
		return false;
	    }

	    // Search according result in child results
	    ResultValue findPredicate = null;
	    if (key.isSubject()) {
		findPredicate = new ResultValue(nodeAttribute,
			actual.getConnectedNodeAttribute());
	    } else {
		// Switch connectedNode and node if it is an object
		findPredicate = new ResultValue(
			actual.getConnectedNodeAttribute(), nodeAttribute);
	    }

	    if (!predicateResults.contains(findPredicate)) {
		return false;
	    }
	}

	return true;
    }

    /**
     * Check if the given field also occurs as subject or object in another
     * triple pattern.
     * 
     * @param field
     *            Field to search for
     * @param bgp
     *            All {@link TriplePattern}s
     * @return True if the field occurs as subject or object
     */
    private static Boolean isSubjectOrObject(String field,
	    List<TriplePattern> bgp) {
	for (TriplePattern tp : bgp) {
	    if (TriplePatternUtils.getSubjectOfPresentation(
		    tp.getStringRepresentation()).equals(field)
		    || TriplePatternUtils.getObjectOfPresentation(
			    tp.getStringRepresentation()).equals(field)) {
		return true;
	    }
	}
	return false;
    }

    /**
     * Get all {@link CompositeKey}s for a given {@link CompositeKey} which have
     * common join variables. This means, that these triple patterns must be
     * fulfilled with the same instantiation of the variable fields.
     * 
     * e.g. ?a foaf:knows ?b and ?a ex:likes ?b. In this case there must be two
     * edges between the ?a and ?b nodes. If you call this method with (?a
     * foaf:knows ?b, SUBJECT) then the result is (?a ex:likes ?b, SUBJECT)
     * 
     * @param key
     *            {@link CompositeKey} which denotes the {@link TriplePattern}
     * @param bgp
     *            All {@link TriplePattern}s
     * @return All {@link CompositeKey}s which have common join variables with
     *         the given {@link CompositeKey}.
     */
    public static Set<CompositeKey> getJoinCompositeKeys(CompositeKey key,
	    List<TriplePattern> bgp) {
	Set<CompositeKey> allDependingKeys = getDependingCompositeKeys(key,
		bgp, true);

	// Variable fields appearing in the triple pattern
	Set<String> joinFields = new HashSet<>();

	if (key.isSubject()) {
	    if (TriplePatternUtils.isVariable(key.getPredicate())) {
		joinFields.add(key.getPredicate());
	    }

	    if (TriplePatternUtils.isVariable(key.getObject())) {
		joinFields.add(key.getObject());
	    }
	} else if (key.isObject()) {
	    if (TriplePatternUtils.isVariable(key.getPredicate())) {
		joinFields.add(key.getPredicate());
	    }

	    if (TriplePatternUtils.isVariable(key.getSubject())) {
		joinFields.add(key.getSubject());
	    }
	} else if (key.isPredicate()) {
	    if (TriplePatternUtils.isVariable(key.getObject())) {
		joinFields.add(key.getObject());
	    }

	    if (TriplePatternUtils.isVariable(key.getSubject())) {
		joinFields.add(key.getSubject());
	    }
	}

	Iterator<CompositeKey> itr = allDependingKeys.iterator();

	/*
	 * Delete all fields that have no field in common with the given one.
	 * Which means there are no join variables
	 */
	while (itr.hasNext()) {
	    CompositeKey candidate = itr.next();
	    if (candidate.isSubject()) {
		if (joinFields.contains(candidate.getPredicate())
			|| joinFields.contains(candidate.getObject())) {
		    // Minimum one field found to join
		    continue;
		} else {
		    itr.remove();
		}

	    } else if (candidate.isObject()) {
		if (joinFields.contains(candidate.getPredicate())
			|| joinFields.contains(candidate.getSubject())) {
		    // Minimum one field found to join
		    continue;
		} else {
		    itr.remove();
		}

	    } else if (candidate.isPredicate()) {
		if (joinFields.contains(candidate.getObject())
			|| joinFields.contains(candidate.getSubject())) {
		    // Minimum one field found to join
		    continue;
		} else {
		    itr.remove();
		}
	    }
	}

	return allDependingKeys;
    }

    /**
     * Get the {@link CompositeKey}s for a specific variable field.
     * 
     * @param key
     *            {@link CompositeKey} of the result, which gives the field
     * @param bgp
     *            All {@link TriplePattern}s
     * @param removeOriginal
     *            Flag if the given key should be removed or not
     * @return Set of {@link CompositeKey}s which must be fulfilled for the
     *         field which is given by the key. The set doesn't contain the
     *         given key.
     */
    public static Set<CompositeKey> getDependingCompositeKeys(CompositeKey key,
	    List<TriplePattern> bgp, Boolean removeOriginal) {
	Set<CompositeKey> result = new HashSet<>();
	String field = key.getField();

	// Iterate through the triple pattern
	for (TriplePattern tp : bgp) {
	    /*
	     * If subject, predicate or object matches the field, then add a new
	     * CompositeKey to the result. A triple pattern could be added more
	     * than once to the result, but with different positions of the
	     * node.
	     */
	    if (tp.getSubject().equals(field)) {
		result.add(new CompositeKey(tp.getStringRepresentation(),
			Position.SUBJECT));
	    }

	    if (tp.getObject().equals(field)) {
		result.add(new CompositeKey(tp.getStringRepresentation(),
			Position.OBJECT));
	    }

	    if (tp.getPredicate().equals(field)) {
		result.add(new CompositeKey(tp.getStringRepresentation(),
			Position.PREDICATE));
	    }
	}

	/*
	 * Remove original key, if the flag is set to true.
	 */
	if (removeOriginal) {
	    result.remove(key);
	}

	return result;
    }

    /**
     * Add the depending triple pattern for the fields which are in the given
     * triple pattern and that are not on the position in the
     * {@link CompositeKey}. The triple pattern of the given
     * {@link CompositeKey} will not be added to the queue.
     * 
     * @param actual
     *            Actual {@link CompositeKey}
     * @param queue
     *            Queue to add the {@link CompositeKey}s
     * @param bgp
     *            All {@link TriplePattern}s
     */
    public static void addDependingFieldsToQueue(CompositeKey actual,
	    Queue<CompositeKey> queue, List<TriplePattern> bgp) {
	Set<String> fields = new HashSet<String>();

	if (actual.isSubject()) {
	    if (TriplePatternUtils.isVariable(actual.getPredicate())) {
		fields.add(actual.getPredicate());
	    }
	    fields.add(actual.getObject());
	} else if (actual.isPredicate()) {
	    fields.add(actual.getSubject());
	    fields.add(actual.getObject());
	} else {
	    // Object
	    fields.add(actual.getSubject());
	    if (TriplePatternUtils.isVariable(actual.getPredicate())) {
		fields.add(actual.getPredicate());
	    }
	}

	for (TriplePattern tp : bgp) {
	    if (fields.contains(tp.getSubject())
		    && !tp.getStringRepresentation().equals(
			    actual.getTriplePattern())) {
		/*
		 * Subject of the triple pattern matches a field and it is not
		 * the same triple pattern which was checked in this round
		 */
		CompositeKey newKey = new CompositeKey(
			tp.getStringRepresentation(), Position.SUBJECT);
		if (!queue.contains(newKey)) {
		    queue.add(newKey);
		}
	    }

	    if (fields.contains(tp.getPredicate())
		    && !tp.getStringRepresentation().equals(
			    actual.getTriplePattern())) {

		CompositeKey newKey = new CompositeKey(
			tp.getStringRepresentation(), Position.PREDICATE);
		if (!queue.contains(newKey)) {
		    queue.add(newKey);
		}

	    }

	    if (fields.contains(tp.getObject())
		    && !tp.getStringRepresentation().equals(
			    actual.getTriplePattern())) {

		CompositeKey newKey = new CompositeKey(
			tp.getStringRepresentation(), Position.OBJECT);
		if (!queue.contains(newKey)) {
		    queue.add(newKey);
		}

	    }
	}
    }

    /**
     * Add all {@link CompositeKey}s to the queue (if they are not added yet)
     * which occur with the given field. All {@link TriplePattern} with the
     * given field will be ignored. So call this method after you have deleted
     * all results of this field. If you call it with "(?a knows ?b, Subject)"
     * and the {@link TriplePattern}s are "?a knows ?b", "?b knows ?c",
     * "?a knows ?x", then only "?b knows ?c" will be added.
     * 
     * @param actual
     *            {@link CompositeKey} which denotes the actual field
     * @param queue
     *            {@link Queue} to add results
     * @param bgp
     *            All {@link TriplePattern}s
     */
    public static void addAllDependingFieldsToQueue(CompositeKey actual,
	    Queue<CompositeKey> queue, List<TriplePattern> bgp) {
	Set<String> dependingFields = new HashSet<>();

	if (!TriplePatternUtils.isVariable(actual.getField())
		&& actual.isPredicate()) {
	    // Not a variable field as predicate
	    return;
	}

	for (CompositeKey candidate : getDependingCompositeKeys(actual, bgp,
		false)) {
	    // Search all fields that appear with the given field
	    if (candidate.isSubject()) {
		dependingFields.add(candidate.getObject());

		if (TriplePatternUtils.isVariable(candidate.getPredicate())) {
		    dependingFields.add(candidate.getPredicate());
		}
	    } else if (candidate.isPredicate()) {
		dependingFields.add(candidate.getObject());

		dependingFields.add(candidate.getSubject());
	    } else if (candidate.isObject()) {
		if (TriplePatternUtils.isVariable(candidate.getPredicate())) {
		    dependingFields.add(candidate.getPredicate());
		}

		dependingFields.add(candidate.getSubject());
	    }
	}

	/*
	 * Remove the field, because this method is called when all occurrences
	 * of this variable field are removed, so this variable field isn't in
	 * the results anymore
	 */
	dependingFields.remove(actual.getField());

	for (TriplePattern tp : bgp) {
	    if (!tp.contains(actual.getField())) {
		// Subject is a candidate field
		if (dependingFields.contains(tp.getSubject())) {
		    CompositeKey possible = new CompositeKey(
			    tp.getStringRepresentation(), Position.SUBJECT);

		    // Not in queue
		    if (!queue.contains(possible)) {
			queue.add(possible);
		    }
		}

		// Predicate is a candidate field
		if (dependingFields.contains(tp.getPredicate())) {
		    CompositeKey possible = new CompositeKey(
			    tp.getStringRepresentation(), Position.PREDICATE);

		    // Not in queue
		    if (!queue.contains(possible)) {
			queue.add(possible);
		    }
		}

		// Object is a candidate field
		if (dependingFields.contains(tp.getObject())) {
		    CompositeKey possible = new CompositeKey(
			    tp.getStringRepresentation(), Position.OBJECT);

		    // Not in queue
		    if (!queue.contains(possible)) {
			queue.add(possible);
		    }
		}
	    }
	}
    }

    /**
     * Check if there are shortcuts of length three for the given key. A
     * shortcut of length three looks like this: ?a knows ?b, ?b knows ?c, ?a
     * knows ?c. Longer shortcuts where the chain is longer may not be checked
     * correctly if there is for example a circle in the graph.
     * 
     * @param key
     *            Actual {@link CompositeKey}
     * @param nodeResults
     *            Results of the node
     * @param childMatches
     *            Results of the children
     * @param parentMatches
     *            results of the parents
     * @param bgp
     *            All {@link TriplePattern}
     * @param nodeAttribute
     *            Attribute of the node
     * @return True if nothing was changed, false if something was deleted
     */
    public static Boolean checkShortcuts(CompositeKey key,
	    Map<CompositeKey, List<ResultValue>> nodeResults,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> childMatches,
	    Map<CompositeKeyNodeAttr, List<ResultValue>> parentMatches,
	    List<TriplePattern> bgp, String nodeAttribute) {

	Boolean changed = false;

	// Results to check for the given key
	List<ResultValue> toCheck = nodeResults.get(key);

	if (toCheck == null || toCheck.isEmpty()) {
	    return false;
	}

	// All shortcuts for the given key
	Set<Shortcut> shortcuts = getShortcuts(bgp, key);

	if (shortcuts.isEmpty()) {
	    // No shortcuts, nothing to check
	    return true;
	}

	Iterator<ResultValue> itr = toCheck.iterator();
	Map<String, String> mapping = new HashMap<>();

	// Check all shortcuts for every result
	while (itr.hasNext()) {
	    ResultValue actual = itr.next();

	    // Mapping from variable fields to values
	    mapping.clear();
	    addValuesToMapping(key, actual, mapping, nodeAttribute);

	    // Check the shortcuts for the result
	    for (Shortcut shortcut : shortcuts) {

		/*
		 * Check the first triple pattern from the shortcut. This is the
		 * one that has a join key in common with the given key
		 */
		CompositeKey tp1 = shortcut.getFirstTP();
		List<ResultValue> secondResults = nodeResults.get(tp1);

		if (secondResults == null || secondResults.isEmpty()) {
		    /*
		     * No results found, exit loop and then the result will be
		     * deleted later
		     */
		    itr.remove();
		    break;
		}

		Boolean foundResult = false;

		/*
		 * Search for the last TriplePattern which closes the loop. This
		 * triple pattern has not the same key as the node in the given
		 * CompositeKey. This means that this result must be searched in
		 * either the parent or child results.
		 */
		for (ResultValue secondRes : secondResults) {

		    addValuesToMapping(tp1, secondRes, mapping, nodeAttribute);
		    CompositeKey tp2 = shortcut.getSecondtTP();

		    List<ResultValue> searchIn;

		    String subject = replaceFieldByValue(mapping,
			    tp2.getSubject());
		    String predicate = replaceFieldByValue(mapping,
			    tp2.getPredicate());
		    String object = replaceFieldByValue(mapping,
			    tp2.getObject());

		    /*
		     * Check if the according result should searched in the
		     * child or parent results. E.g. ?a knows ?b, ?b knows ?c,
		     * ?a knows ?c. If you call the method with (?a knows ?b,
		     * SUBJECT) then it should be searched in the child results,
		     * because ?b is the child of ?a. If you call it with (?a
		     * knows ?b, OBJECT) then (?a knows ?c, SUBJECT) should be
		     * searched in the parent results.
		     */
		    if (searchParents(key, tp1, tp2)) {
			if (parentMatches == null) {
			    return false;
			}
			searchIn = parentMatches.get(new CompositeKeyNodeAttr(
				tp2, subject));
		    } else {
			if (childMatches == null) {
			    return false;
			}
			searchIn = childMatches.get(new CompositeKeyNodeAttr(
				tp2, subject));
		    }

		    if (searchIn == null || searchIn.isEmpty()) {
			// Result not found, will be deleted after the loop
			continue;
		    }

		    // Search the result
		    for (ResultValue resVal : searchIn) {
			// Check if the object matches
			if (resVal.getConnectedNodeAttribute().equals(object)) {

			    /*
			     * Check if the predicate is a variable field, if
			     * yes then a result is found. If not then the edge
			     * attribute has to match the predicate.
			     */
			    if (TriplePatternUtils.isVariable(predicate)) {
				foundResult = true;
				break;
			    } else if (resVal.getEdgeAttribute().equals(
				    predicate)) {
				foundResult = true;
				break;
			    }
			}
		    }

		}

		// Remove the result if no result for the shortcut was found.
		if (!foundResult) {
		    itr.remove();
		    changed = true;
		    break;
		}
	    }

	}

	return !changed;
    }

    /**
     * Check if the third triple pattern of the shortcut should be searched in
     * the parent results or not.
     * 
     * @param tp1
     *            First {@link TriplePattern} of the shortcut
     * @param tp2
     *            Second {@link TriplePattern} of the shortcut which contains
     *            also the one same node as tp1
     * @param tp3
     *            Third {@link TriplePattern} which does not contain the same
     *            node as tp1 and tp2
     * @return True if the parent result should be searched for
     */
    private static Boolean searchParents(CompositeKey tp1, CompositeKey tp2,
	    CompositeKey tp3) {
	Set<String> parents = new HashSet<>();
	if (tp1.isObject()) {
	    parents.add(TriplePatternUtils.getSubjectOfPresentation(tp1
		    .getTriplePattern()));
	}

	if (tp2.isObject()) {
	    parents.add(TriplePatternUtils.getSubjectOfPresentation(tp2
		    .getTriplePattern()));
	}

	return parents.contains(TriplePatternUtils.getSubjectOfPresentation(tp3
		.getTriplePattern()));
    }

    /**
     * Add the values to the given mapping, from variable fields to values.
     * 
     * @param key
     *            Actual key
     * @param actual
     *            Actual result
     * @param mapping
     *            Mapping to add key value pairs
     * @param nodeAttribute
     *            Attribute of the node
     */
    private static void addValuesToMapping(CompositeKey key,
	    ResultValue actual, Map<String, String> mapping,
	    String nodeAttribute) {
	/*
	 * Put the values into the mapping with the variable fields
	 */
	if (key.isSubject()) {
	    mapping.put(key.getSubject(), nodeAttribute);
	    mapping.put(key.getObject(), actual.getConnectedNodeAttribute());
	    mapping.put(key.getPredicate(), actual.getEdgeAttribute());
	} else if (key.isObject()) {
	    mapping.put(key.getSubject(), actual.getConnectedNodeAttribute());
	    mapping.put(key.getObject(), nodeAttribute);
	    mapping.put(key.getPredicate(), actual.getEdgeAttribute());
	}
    }

    /**
     * Given a key, search all shortcuts of the form ?a knows ?b, ?b knows ?c,
     * ?a knows ?c.
     * 
     * @param bgp
     *            All {@link TriplePattern}s
     * @param key
     *            Key to search for shortcuts
     * @return Set of {@link Shortcut}s with the two other triple pattern that
     *         must be fulfilled
     */
    public static Set<Shortcut> getShortcuts(List<TriplePattern> bgp,
	    CompositeKey key) {
	Set<CompositeKey> dependingKeys = getDependingCompositeKeys(key, bgp,
		true);
	Set<Shortcut> result = new HashSet<>();

	for (CompositeKey depending : dependingKeys) {
	    // Don't check predicates
	    if (!key.isPredicate() && !depending.isPredicate()) {
		// Other field of the key
		String otherField = "";

		// Other field of the depending key
		String otherField2 = "";

		if (key.isSubject()) {
		    otherField = TriplePatternUtils.getObjectOfPresentation(key
			    .getTriplePattern());

		} else if (key.isObject()) {
		    otherField = TriplePatternUtils
			    .getSubjectOfPresentation(key.getTriplePattern());

		}
		if (depending.isSubject()) {
		    otherField2 = TriplePatternUtils
			    .getObjectOfPresentation(depending
				    .getTriplePattern());
		} else if (depending.isObject()) {
		    otherField2 = TriplePatternUtils
			    .getSubjectOfPresentation(depending
				    .getTriplePattern());
		}

		/*
		 * Search for a connection in the fields of the two keys, to
		 * search for a shortcut
		 */
		CompositeKey connection = getConnectionBetween(otherField,
			otherField2, bgp);
		if (connection != null
			&& isShortcut(key, depending, connection)) {
		    result.add(new Shortcut(depending, connection));
		}

		connection = getConnectionBetween(otherField2, otherField, bgp);
		if (connection != null
			&& isShortcut(key, depending, connection)) {
		    result.add(new Shortcut(depending, connection));
		}
	    }
	}
	return result;
    }

    /**
     * Check that the three given {@link TriplePattern} that describe a
     * shortcut. There has to be 2 different subjects and 2 different objects in
     * the sum of all {@link TriplePattern}. Otherwise it isn't a shortcut.
     * 
     * @param tp1
     *            First {@link TriplePattern}
     * @param tp2
     *            Second {@link TriplePattern}
     * @param tp3
     *            Third {@link TriplePattern}
     * @return True if the three {@link TriplePattern} describe a shortcut.
     */
    private static Boolean isShortcut(CompositeKey tp1, CompositeKey tp2,
	    CompositeKey tp3) {
	Map<String, Integer> subjectCount = new HashMap<>();
	Map<String, Integer> objectCount = new HashMap<>();
	addCountToMap(TriplePatternUtils.getSubjectOfPresentation(tp1
		.getTriplePattern()), subjectCount);
	addCountToMap(TriplePatternUtils.getObjectOfPresentation(tp1
		.getTriplePattern()), objectCount);

	addCountToMap(TriplePatternUtils.getSubjectOfPresentation(tp2
		.getTriplePattern()), subjectCount);
	addCountToMap(TriplePatternUtils.getObjectOfPresentation(tp2
		.getTriplePattern()), objectCount);

	addCountToMap(TriplePatternUtils.getSubjectOfPresentation(tp3
		.getTriplePattern()), subjectCount);
	addCountToMap(TriplePatternUtils.getObjectOfPresentation(tp3
		.getTriplePattern()), objectCount);

	if (subjectCount.size() != 2) {
	    return false;
	}

	if (objectCount.size() != 2) {
	    return false;
	}
	return true;
    }

    private static void addCountToMap(String toAdd, Map<String, Integer> count) {
	if (count.get(toAdd) == null) {
	    count.put(toAdd, 1);
	} else {
	    count.put(toAdd, count.get(toAdd) + 1);
	}
    }

    /**
     * Searches for a connection between the given field1 and field2 in the
     * basic graph pattern.
     * 
     * @param field1
     *            First field
     * @param field2
     *            Second field
     * @param bgp
     *            All triple pattern
     * @return A connecting triple pattern if there is one, an arbitrary one if
     *         there are more or null if there are none
     */
    private static CompositeKey getConnectionBetween(String field1,
	    String field2, List<TriplePattern> bgp) {
	for (TriplePattern tp : bgp) {
	    if (tp.getSubject().equals(field1) && tp.getObject().equals(field2)) {
		return new CompositeKey(tp.getStringRepresentation(),
			Position.SUBJECT);
	    }
	}
	return null;
    }
}
