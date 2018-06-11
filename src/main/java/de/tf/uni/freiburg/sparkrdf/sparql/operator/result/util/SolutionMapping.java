package de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePatternUtils;

/**
 * Represents one solution mapping
 * 
 * @author Thorsten Berberich
 * 
 */
public class SolutionMapping implements Serializable {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = 2030201698394751809L;

    /**
     * Mapping from the variable fields of a result line to the real values of
     * that field
     */
    private final Map<String, String> fieldValueMapping;

    /**
     * The {@link TriplePattern} which was stored in this result first
     */
    private final String initialTriplePattern;

    /**
     * All stored variables in this result
     */
    private Set<String> storedVariables;

    /**
     * Create a new result line
     * 
     * @param initialTriplePattern
     *            {@link TriplePattern} which is stored first in this result
     */
    public SolutionMapping(String initialTriplePattern) {
	fieldValueMapping = new HashMap<>();
	this.initialTriplePattern = initialTriplePattern;
    }

    /**
     * Add a mapping to this result line
     * 
     * @param field
     *            The variable field of the first result
     * @param value
     *            The value of the first result
     */
    public void addMapping(String field, String value) {
	if (storedVariables == null) {
	    storedVariables = new HashSet<>();
	}

	if (!TriplePatternUtils.isVariable(field)) {
	    // Not a variable field
	    return;
	}

	if (!storedVariables.contains(field)) {
	    storedVariables.add(field);
	}

	if (!fieldValueMapping.containsKey(field)) {
	    if (value != null) {
		fieldValueMapping.put(field, value);
	    }
	}
    }

    /**
     * Project this result to the given variables, which means after this
     * operation this solution mapping contains only the variables which are in
     * the given list
     * 
     * @param toProject
     *            Variables to keep
     */
    public void project(Set<String> toProject) {
	storedVariables.removeAll(toProject);
	for (String storedVariable : storedVariables) {
	    fieldValueMapping.remove(storedVariable);
	}
	storedVariables.clear();
	storedVariables.addAll(toProject);
    }

    /**
     * Add a all mappings to this result line
     * 
     * @param toAdd
     *            Mappings to add
     */
    public void addAllMappings(Map<String, String> toAdd) {
	if (storedVariables == null) {
	    storedVariables = new HashSet<>();
	}
	for (Entry<String, String> entry : toAdd.entrySet()) {
	    addMapping(entry.getKey(), entry.getValue());
	}
    }

    /**
     * Get all mappings of this result
     * 
     * @return Mapping
     */
    public Map<String, String> getAllMappings() {
	return fieldValueMapping;
    }

    /**
     * Get the value to the given field.
     * 
     * @param field
     *            Variable field to get the value
     * @return The value {@link TriplePattern} or null if the variable was not
     *         found
     * @see {@link Map#get(Object)}
     */
    public String getValueToField(String field) {
	return fieldValueMapping.get(field);
    }

    /**
     * Get the initial {@link TriplePattern}
     * 
     * @return Initial {@link TriplePattern}
     */
    public String getInitialTriplePattern() {
	return initialTriplePattern;
    }

    /**
     * Get all variable fields that are stored in this result
     * 
     * @return Set of all variable field
     */
    public Set<String> getStoredVariables() {
	return storedVariables;
    }

    /**
     * Get the join variable between the variable fields that are stored in this
     * result and the given {@link TriplePattern}
     * 
     * @param tp
     *            {@link TriplePattern}
     * @return Set of join variables
     */
    public Set<String> getJoinVariables(TriplePattern tp) {
	Set<String> tpVars = TriplePatternUtils.getVariablesOfTriplePattern(tp);
	Set<String> fulfilledCopy = new HashSet<>(storedVariables);
	tpVars.retainAll(fulfilledCopy);
	return tpVars;
    }

    @Override
    public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime
		* result
		+ ((fieldValueMapping == null) ? 0 : fieldValueMapping
			.hashCode());
	result = prime
		* result
		+ ((initialTriplePattern == null) ? 0 : initialTriplePattern
			.hashCode());
	result = prime * result
		+ ((storedVariables == null) ? 0 : storedVariables.hashCode());
	return result;
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (!(obj instanceof SolutionMapping))
	    return false;
	SolutionMapping other = (SolutionMapping) obj;
	if (fieldValueMapping == null) {
	    if (other.fieldValueMapping != null)
		return false;
	} else if (!fieldValueMapping.equals(other.fieldValueMapping))
	    return false;
	if (initialTriplePattern == null) {
	    if (other.initialTriplePattern != null)
		return false;
	} else if (!initialTriplePattern.equals(other.initialTriplePattern))
	    return false;
	if (storedVariables == null) {
	    if (other.storedVariables != null)
		return false;
	} else if (!storedVariables.equals(other.storedVariables))
	    return false;
	return true;
    }

    @Override
    public String toString() {
	StringBuilder firstLine = new StringBuilder();
	StringBuilder secondLine = new StringBuilder();

	for (Entry<String, String> entry : fieldValueMapping.entrySet()) {
	    if (firstLine.length() == 0) {
		firstLine.append(entry.getKey());
		secondLine.append(entry.getValue());
	    } else {
		firstLine.append("\t" + entry.getKey());
		secondLine.append("\t" + entry.getValue());
	    }
	}
	firstLine.append("\n");
	firstLine.append(secondLine);
	return firstLine.toString();
    }
}
