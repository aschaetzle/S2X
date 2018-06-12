import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePatternUtils;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.PartialResult;

/**
 * @author Thorsten Berberich
 */
public class TriplePatternUtilsTest {

    @Test
    public void testGetTriplePatternsForResult() {
	TriplePattern tp1 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp2 = new TriplePattern("?b", "knows", "?c");
	TriplePattern tp3 = new TriplePattern("?b", "knows", "?a");
	TriplePattern tp4 = new TriplePattern("?t", "knows", "X");
	TriplePattern tp5 = new TriplePattern("a", "?t", "Y");

	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);

	Set<TriplePattern> result = TriplePatternUtils
		.getTriplePatternsForResult(bgp);

	Set<TriplePattern> expected = new HashSet<>();
	expected.add(tp1);
	expected.add(tp2);
	expected.add(tp4);

	assertEquals(expected, result);

	TriplePattern tp6 = new TriplePattern("?a", "?t", "?b");
	bgp.add(tp6);

	// Test 2
	bgp.clear();
	bgp.add(tp6);
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);

	expected.clear();
	expected.add(tp6);
	expected.add(tp2);

	result = TriplePatternUtils.getTriplePatternsForResult(bgp);

	assertEquals(expected, result);

	TriplePattern tp11 = new TriplePattern("?x", "knows", "?c");
	TriplePattern tp21 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp31 = new TriplePattern("?c", "knows", "?y");

	bgp.clear();
	bgp.add(tp11);
	bgp.add(tp21);
	bgp.add(tp31);

	expected.clear();
	expected.add(tp11);
	expected.add(tp21);
	expected.add(tp31);

	result = TriplePatternUtils.getTriplePatternsForResult(bgp);

	assertEquals(expected, result);
    }

    @Test
    public void getPartialResults() {
	TriplePattern tp1 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp2 = new TriplePattern("?b", "knows", "?c");
	TriplePattern tp3 = new TriplePattern("?b", "knows", "?a");
	TriplePattern tp4 = new TriplePattern("?t", "knows", "X");
	TriplePattern tp5 = new TriplePattern("a", "?t", "Y");

	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);

	List<PartialResult> result = TriplePatternUtils
		.getPartialResults(TriplePatternUtils
			.getTriplePatternsForResult(bgp));

	List<PartialResult> expected = new ArrayList<>();
	PartialResult exp = new PartialResult();
	exp.addTriplePattern(tp2);
	exp.addTriplePattern(tp1);
	PartialResult exp2 = new PartialResult();
	exp2.addTriplePattern(tp4);
	expected.add(exp);
	expected.add(exp2);

	assertEquals(expected, result);

	TriplePattern tp6 = new TriplePattern("?a", "?t", "?b");

	// Test 2
	bgp.clear();
	bgp.add(tp6);
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);

	expected.clear();
	exp = new PartialResult();
	exp.addTriplePattern(tp6);
	exp.addTriplePattern(tp2);
	expected.add(exp);

	result = TriplePatternUtils.getPartialResults(TriplePatternUtils
		.getTriplePatternsForResult(bgp));

	assertEquals(expected, result);

	// Test 3
	TriplePattern tp11 = new TriplePattern("?x", "knows", "?c");
	TriplePattern tp21 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp31 = new TriplePattern("?c", "knows", "?y");

	bgp.clear();

	bgp.add(tp11);
	bgp.add(tp21);
	bgp.add(tp31);

	expected.clear();
	exp = new PartialResult();
	exp.addTriplePattern(tp11);
	exp.addTriplePattern(tp31);
	exp2 = new PartialResult();
	exp2.addTriplePattern(tp21);
	expected.add(exp);
	expected.add(exp2);

	result = TriplePatternUtils.getPartialResults(TriplePatternUtils
		.getTriplePatternsForResult(bgp));

	assertEquals(expected, result);

	// Test 4
	TriplePattern tp41 = new TriplePattern("?d", "knows", "?e");
	TriplePattern tp51 = new TriplePattern("?e", "knows", "?f");

	bgp.add(tp51);
	bgp.add(tp41);

	expected.clear();
	exp = new PartialResult();
	exp.addTriplePattern(tp11);
	exp.addTriplePattern(tp31);

	exp2 = new PartialResult();
	exp2.addTriplePattern(tp21);

	PartialResult exp3 = new PartialResult();
	exp3 = new PartialResult();
	exp3.addTriplePattern(tp51);
	exp3.addTriplePattern(tp41);

	expected.add(exp3);
	expected.add(exp);
	expected.add(exp2);

	result = TriplePatternUtils.getPartialResults(TriplePatternUtils
		.getTriplePatternsForResult(bgp));

	assertEquals(expected, result);
    }

    @Test
    public void sortPartialResults() {
	PartialResult p1 = new PartialResult();
	p1.addTriplePattern(new TriplePattern("?a", "knows", "?b"));
	p1.addTriplePattern(new TriplePattern("?x", "knows", "?y"));
	p1.addTriplePattern(new TriplePattern("?b", "knows", "?x"));

	PartialResult p2 = new PartialResult();
	p2.addTriplePattern(new TriplePattern("?b", "?t", "?x"));
	p2.addTriplePattern(new TriplePattern("?a", "knows", "?z"));
	p2.addTriplePattern(new TriplePattern("?t", "knows", "?y"));
	p2.addTriplePattern(new TriplePattern("?y", "knows", "?a"));

	List<PartialResult> results = new ArrayList<>();
	results.add(p1);
	results.add(p2);

	List<PartialResult> expected = new ArrayList<>();

	PartialResult e1 = new PartialResult();
	e1.addTriplePattern(new TriplePattern("?a", "knows", "?b"));
	e1.addTriplePattern(new TriplePattern("?b", "knows", "?x"));
	e1.addTriplePattern(new TriplePattern("?x", "knows", "?y"));

	PartialResult e2 = new PartialResult();
	e2.addTriplePattern(new TriplePattern("?b", "?t", "?x"));
	e2.addTriplePattern(new TriplePattern("?t", "knows", "?y"));
	e2.addTriplePattern(new TriplePattern("?y", "knows", "?a"));
	e2.addTriplePattern(new TriplePattern("?a", "knows", "?z"));

	expected.add(e1);
	expected.add(e2);

	TriplePatternUtils.sortPartialResults(results);
	assertEquals(expected, results);
    }

    @Test
    public void isObjectLiteral() {
	TriplePattern tp1 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp2 = new TriplePattern("?b", "knows", "?c");
	TriplePattern tp3 = new TriplePattern("?a", "knows", "?c");
	TriplePattern tp4 = new TriplePattern("?t", "knows", "X");
	TriplePattern tp5 = new TriplePattern("a", "?t", "Y");

	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);

	assertFalse(TriplePatternUtils.isObjectLiteral(bgp, "?b"));
	assertFalse(TriplePatternUtils.isObjectLiteral(bgp, "?a"));
	assertFalse(TriplePatternUtils.isObjectLiteral(bgp, "?c"));
	assertTrue(TriplePatternUtils.isObjectLiteral(bgp, "X"));
	assertTrue(TriplePatternUtils.isObjectLiteral(bgp, "Y"));
	assertFalse(TriplePatternUtils.isObjectLiteral(bgp, "?t"));

	TriplePattern tp11 = new TriplePattern("?X", "rdf:type", "ub:Student");
	TriplePattern tp21 = new TriplePattern("?Y", "rdf:type",
		"ub:Department");
	TriplePattern tp31 = new TriplePattern("?X", "ub:memberOf", "?Y");
	TriplePattern tp41 = new TriplePattern("?Y", "ub:subOrganizationOf",
		"<http://www.University0.edu>");
	TriplePattern tp51 = new TriplePattern("?X", "ub:emailAddress", "?Z");

	bgp = new ArrayList<TriplePattern>();
	bgp.add(tp11);
	bgp.add(tp21);
	bgp.add(tp31);
	bgp.add(tp41);
	bgp.add(tp51);

	assertFalse(TriplePatternUtils.isObjectLiteral(bgp, "?X"));
	assertTrue(TriplePatternUtils.isObjectLiteral(bgp, "ub:Student"));
	assertTrue(TriplePatternUtils.isObjectLiteral(bgp, "ub:Department"));
	assertFalse(TriplePatternUtils.isObjectLiteral(bgp, "?Y"));
    }
}
