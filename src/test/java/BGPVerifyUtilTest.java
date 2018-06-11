import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.junit.Test;

import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKey;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.CompositeKeyNodeAttr;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.Position;
import de.tf.uni.freiburg.sparkrdf.model.rdf.result.ResultValue;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp.util.BGPVerifyUtil;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.bgp.util.Shortcut;

/**
 * @author Thorsten Berberich
 */
public class BGPVerifyUtilTest {

    private final TriplePattern tp1 = new TriplePattern("?a", "knows", "?b");
    private final TriplePattern tp2 = new TriplePattern("?b", "knows", "?c");
    private final TriplePattern tp3 = new TriplePattern("?a", "knows", "?c");
    private final TriplePattern tp4 = new TriplePattern("?a", "knows", "X");
    private final TriplePattern tp5 = new TriplePattern("?t", "knows", "X");
    private final TriplePattern tp6 = new TriplePattern("?b", "?t", "Y");
    private final TriplePattern tp7 = new TriplePattern("?alone", "knows",
	    "?gg");
    private final TriplePattern tp8 = new TriplePattern("?a", "knows", "?a");
    private final TriplePattern tp9 = new TriplePattern("?e", "?f", "?h");

    @Test
    public void getDependingCompositeKeys() {
	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);
	bgp.add(tp6);
	bgp.add(tp7);
	CompositeKey key = new CompositeKey(tp1.getStringRepresentation(),
		Position.SUBJECT);

	Set<CompositeKey> res = BGPVerifyUtil.getDependingCompositeKeys(key,
		bgp, true);

	// Test 1 - Star pattern like
	Set<CompositeKey> expected = new HashSet<>();
	expected.add(new CompositeKey(tp3.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp4.getStringRepresentation(),
		Position.SUBJECT));

	assertEquals(expected, res);
	expected.clear();

	// Test 2 - Star pattern like other TP
	key = new CompositeKey(tp3.getStringRepresentation(), Position.SUBJECT);
	res = BGPVerifyUtil.getDependingCompositeKeys(key, bgp, true);
	expected.add(new CompositeKey(tp1.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp4.getStringRepresentation(),
		Position.SUBJECT));

	assertEquals(expected, res);
	expected.clear();

	// Test 3 - Chain pattern
	key = new CompositeKey(tp1.getStringRepresentation(), Position.OBJECT);
	res = BGPVerifyUtil.getDependingCompositeKeys(key, bgp, true);
	expected.add(new CompositeKey(tp2.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp6.getStringRepresentation(),
		Position.SUBJECT));

	assertEquals(expected, res);
	expected.clear();

	// Test 4 - Predicate variable field
	key = new CompositeKey(tp5.getStringRepresentation(), Position.SUBJECT);
	res = BGPVerifyUtil.getDependingCompositeKeys(key, bgp, true);
	expected.add(new CompositeKey(tp6.getStringRepresentation(),
		Position.PREDICATE));

	assertEquals(expected, res);
	expected.clear();

	// Test 5 - Predicate variable field
	key = new CompositeKey(tp6.getStringRepresentation(),
		Position.PREDICATE);
	res = BGPVerifyUtil.getDependingCompositeKeys(key, bgp, true);
	expected.add(new CompositeKey(tp5.getStringRepresentation(),
		Position.SUBJECT));

	assertEquals(expected, res);
	expected.clear();

	// Test 6 - Empty set
	key = new CompositeKey(tp7.getStringRepresentation(), Position.SUBJECT);
	res = BGPVerifyUtil.getDependingCompositeKeys(key, bgp, true);

	assertEquals(expected, res);
	expected.clear();
    }

    @Test
    public void checkTriplePatternGroup() {
	TriplePattern tp11 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp21 = new TriplePattern("?b", "knows", "?c");
	TriplePattern tp31 = new TriplePattern("?a", "knows", "?c");
	TriplePattern tp41 = new TriplePattern("?t", "knows", "X");
	TriplePattern tp51 = new TriplePattern("?b", "?t", "Y");

	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	bgp.add(tp11);
	bgp.add(tp21);
	bgp.add(tp31);
	bgp.add(tp41);
	bgp.add(tp51);

	Map<CompositeKey, List<ResultValue>> nodeResults = new HashMap<CompositeKey, List<ResultValue>>();
	List<ResultValue> l1 = new ArrayList<>();
	l1.add(new ResultValue("knows", "b"));
	List<ResultValue> l2 = new ArrayList<>();
	l2.add(new ResultValue("knows", "b2"));
	nodeResults.put(new CompositeKey(tp31.getStringRepresentation(),
		Position.SUBJECT), l1);
	nodeResults.put(new CompositeKey(tp11.getStringRepresentation(),
		Position.SUBJECT), l2);

	// Correct result
	assertEquals(true, BGPVerifyUtil.checkTriplePatternGroup(
		new CompositeKey(tp11.getStringRepresentation(),
			Position.SUBJECT), nodeResults, bgp));
	assertEquals(true, BGPVerifyUtil.checkTriplePatternGroup(
		new CompositeKey(tp31.getStringRepresentation(),
			Position.SUBJECT), nodeResults, bgp));

	// Result missing
	assertEquals(false, BGPVerifyUtil.checkTriplePatternGroup(
		new CompositeKey(tp11.getStringRepresentation(),
			Position.OBJECT), nodeResults, bgp));

	// Result empty
	nodeResults.clear();
	l1.clear();
	l1.add(new ResultValue("likes", "Y"));
	nodeResults.put(new CompositeKey(tp51.getStringRepresentation(),
		Position.PREDICATE), l1);
	nodeResults.put(new CompositeKey(tp41.getStringRepresentation(),
		Position.SUBJECT), new ArrayList<ResultValue>());

	assertEquals(false, BGPVerifyUtil.checkTriplePatternGroup(
		new CompositeKey(tp51.getStringRepresentation(),
			Position.PREDICATE), nodeResults, bgp));

	// Result null
	nodeResults.remove(new CompositeKey(tp41.getStringRepresentation(),
		Position.SUBJECT));
	assertEquals(false, BGPVerifyUtil.checkTriplePatternGroup(
		new CompositeKey(tp51.getStringRepresentation(),
			Position.PREDICATE), nodeResults, bgp));
    }

    @Test
    public void addAllDependingFieldsToQueue() {
	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);
	bgp.add(tp6);
	bgp.add(tp7);
	bgp.add(tp8);

	Queue<CompositeKey> testQueue = new LinkedList<CompositeKey>();

	// Empty result
	BGPVerifyUtil.addAllDependingFieldsToQueue(
		new CompositeKey(tp7.getStringRepresentation(),
			Position.SUBJECT), testQueue, bgp);

	assertEquals(true, testQueue.isEmpty());

	BGPVerifyUtil.addAllDependingFieldsToQueue(
		new CompositeKey(tp7.getStringRepresentation(),
			Position.PREDICATE), testQueue, bgp);

	assertEquals(true, testQueue.isEmpty());
	testQueue.clear();

	// Results added
	BGPVerifyUtil.addAllDependingFieldsToQueue(
		new CompositeKey(tp1.getStringRepresentation(),
			Position.SUBJECT), testQueue, bgp);
	Queue<CompositeKey> expectedQueue = new LinkedList<>();
	expectedQueue.add(new CompositeKey(tp2.getStringRepresentation(),
		Position.SUBJECT));
	expectedQueue.add(new CompositeKey(tp2.getStringRepresentation(),
		Position.OBJECT));
	expectedQueue.add(new CompositeKey(tp6.getStringRepresentation(),
		Position.SUBJECT));
	expectedQueue.add(new CompositeKey(tp5.getStringRepresentation(),
		Position.OBJECT));

	assertQueue(expectedQueue, testQueue);
	testQueue.clear();

	// Variable predicate test
	BGPVerifyUtil.addAllDependingFieldsToQueue(
		new CompositeKey(tp6.getStringRepresentation(),
			Position.PREDICATE), testQueue, bgp);
	expectedQueue.clear();
	expectedQueue.add(new CompositeKey(tp1.getStringRepresentation(),
		Position.OBJECT));
	expectedQueue.add(new CompositeKey(tp2.getStringRepresentation(),
		Position.SUBJECT));
	expectedQueue.add(new CompositeKey(tp4.getStringRepresentation(),
		Position.OBJECT));

	assertQueue(expectedQueue, testQueue);
	testQueue.clear();

	// Results added
	BGPVerifyUtil
		.addAllDependingFieldsToQueue(
			new CompositeKey(tp1.getStringRepresentation(),
				Position.OBJECT), testQueue, bgp);
	expectedQueue.clear();
	expectedQueue.add(new CompositeKey(tp3.getStringRepresentation(),
		Position.SUBJECT));
	expectedQueue.add(new CompositeKey(tp3.getStringRepresentation(),
		Position.OBJECT));
	expectedQueue.add(new CompositeKey(tp4.getStringRepresentation(),
		Position.SUBJECT));
	expectedQueue.add(new CompositeKey(tp5.getStringRepresentation(),
		Position.SUBJECT));
	expectedQueue.add(new CompositeKey(tp8.getStringRepresentation(),
		Position.OBJECT));
	expectedQueue.add(new CompositeKey(tp8.getStringRepresentation(),
		Position.SUBJECT));

	assertQueue(expectedQueue, testQueue);
	testQueue.clear();
    }

    @Test
    public void checkAdjacentConnections() {
	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);
	bgp.add(tp6);
	bgp.add(tp7);
	bgp.add(tp8);
	bgp.add(tp9);
	bgp.add(new TriplePattern("?f", "knows", "Abc"));

	// NodeTest Results
	Map<CompositeKey, List<ResultValue>> nodeResults = new HashMap<>();
	ArrayList<ResultValue> result = new ArrayList<>();
	result.add(new ResultValue("knows", "Peter"));
	result.add(new ResultValue("knows", "NotExisting"));

	CompositeKey nodeKey = new CompositeKey(tp1.getStringRepresentation(),
		Position.SUBJECT);
	nodeResults.put(nodeKey, result);

	// Child results
	Map<CompositeKeyNodeAttr, List<ResultValue>> childResults = new HashMap<>();
	ArrayList<ResultValue> childResult = new ArrayList<>();
	childResult.add(new ResultValue("knows", "Hans"));

	CompositeKeyNodeAttr childKey = new CompositeKeyNodeAttr(
		tp1.getStringRepresentation(), Position.OBJECT, "Peter");

	childResults.put(childKey, childResult);

	// Parent results
	Map<CompositeKeyNodeAttr, List<ResultValue>> parentResults = new HashMap<>();

	// Expected
	Map<CompositeKey, List<ResultValue>> expected = new HashMap<>();
	ArrayList<ResultValue> expectedResult = new ArrayList<>();
	expectedResult.add(new ResultValue("knows", "Peter"));

	expected.put(nodeKey, expectedResult);

	// Delete a subject result
	assertFalse(BGPVerifyUtil.checkAdjacentConnections(nodeKey,
		nodeResults, parentResults, childResults, bgp, "Hans"));

	assertEquals(expected, nodeResults);

	// Delete an object result
	nodeResults.clear();
	result.clear();
	nodeKey = new CompositeKey(tp1.getStringRepresentation(),
		Position.OBJECT);

	result.add(new ResultValue("knows", "Maria"));
	result.add(new ResultValue("knows", "NotExisting"));

	nodeResults.put(nodeKey, result);

	parentResults.clear();

	ArrayList<ResultValue> parentResult = new ArrayList<>();
	parentResult.add(new ResultValue("knows", "Hans"));
	CompositeKeyNodeAttr parentKey = new CompositeKeyNodeAttr(
		tp1.getStringRepresentation(), Position.SUBJECT, "Maria");
	parentResults.put(parentKey, parentResult);

	expected.clear();
	expectedResult.clear();
	expectedResult.add(new ResultValue("knows", "Maria"));

	expected.put(nodeKey, expectedResult);

	assertFalse(BGPVerifyUtil.checkAdjacentConnections(nodeKey,
		nodeResults, parentResults, childResults, bgp, "Hans"));

	// Delete one subject result
	assertEquals(expected, nodeResults);

	nodeResults.clear();
	result.clear();
	nodeKey = new CompositeKey(tp9.getStringRepresentation(),
		Position.SUBJECT);

	result.add(new ResultValue("likes", "Hans"));
	result.add(new ResultValue("knows", "Hans"));

	nodeResults.put(nodeKey, result);

	childResults.clear();
	childResult.clear();

	childResults.put(new CompositeKeyNodeAttr(
		tp9.getStringRepresentation(), Position.OBJECT, "Hans"),
		childResult);

	List<ResultValue> childPredResult = new ArrayList<>();
	childPredResult.add(new ResultValue("Peter", "Hans"));

	childResults.put(new CompositeKeyNodeAttr(
		tp9.getStringRepresentation(), Position.PREDICATE, "knows"),
		childPredResult);

	expected.clear();
	expectedResult.clear();

	expectedResult.add(new ResultValue("knows", "Hans"));

	expected.put(nodeKey, expectedResult);

	assertFalse(BGPVerifyUtil.checkAdjacentConnections(nodeKey,
		nodeResults, parentResults, childResults, bgp, "Peter"));

	// Delete result because of missing variable predicate
	assertEquals(expected, nodeResults);

	nodeResults.clear();
	result.clear();

	nodeKey = new CompositeKey(tp9.getStringRepresentation(),
		Position.PREDICATE);

	result.add(new ResultValue("Peter", "Hans"));
	result.add(new ResultValue("Maria", "Hans"));

	nodeResults.put(nodeKey, result);

	parentResults.clear();
	parentResult.clear();

	parentResult.add(new ResultValue("knows", "Hans"));

	parentResults.put(
		new CompositeKeyNodeAttr(tp9.getStringRepresentation(),
			Position.SUBJECT, "Peter"), parentResult);

	childResult.clear();
	childResults.clear();

	childResult.add(new ResultValue("knows", "Peter"));

	childResults.put(new CompositeKeyNodeAttr(
		tp9.getStringRepresentation(), Position.OBJECT, "Hans"),
		childResult);

	expected.clear();
	expectedResult.clear();

	expectedResult.add(new ResultValue("Peter", "Hans"));

	expected.put(nodeKey, expectedResult);

	assertFalse(BGPVerifyUtil.checkAdjacentConnections(nodeKey,
		nodeResults, parentResults, childResults, bgp, "knows"));

	// Check the variable predicate node
	assertEquals(expected, nodeResults);

	// Nothing to change
	assertTrue(BGPVerifyUtil.checkAdjacentConnections(nodeKey, nodeResults,
		parentResults, childResults, bgp, "knows"));

	// Check the variable predicate node
	assertEquals(expected, nodeResults);

	// Test: Nothing to change

	nodeResults.clear();
	result.clear();
	nodeKey = new CompositeKey(tp9.getStringRepresentation(),
		Position.SUBJECT);

	result.add(new ResultValue("knows", "Hans"));

	nodeResults.put(nodeKey, result);

	childResults.clear();
	childResult.clear();

	childPredResult.clear();
	childPredResult.add(new ResultValue("Peter", "Hans"));

	childResults.put(new CompositeKeyNodeAttr(
		tp9.getStringRepresentation(), Position.PREDICATE, "knows"),
		childPredResult);

	expected.clear();
	expectedResult.clear();

	expectedResult.add(new ResultValue("knows", "Hans"));

	expected.put(nodeKey, expectedResult);

	assertTrue(BGPVerifyUtil.checkAdjacentConnections(nodeKey, nodeResults,
		parentResults, childResults, bgp, "Peter"));

	assertEquals(expected, nodeResults);
    }

    @Test
    public void addDependingFieldsToQueue() {
	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	bgp.add(tp1);
	bgp.add(tp2);
	bgp.add(tp3);
	bgp.add(tp4);
	bgp.add(tp5);
	bgp.add(tp6);
	bgp.add(tp7);
	bgp.add(tp8);

	// Subject test
	CompositeKey actual = new CompositeKey(tp1.getStringRepresentation(),
		Position.SUBJECT);
	Queue<CompositeKey> queue = new LinkedList<>();

	BGPVerifyUtil.addDependingFieldsToQueue(actual, queue, bgp);

	Queue<CompositeKey> expected = new LinkedList<>();

	expected.add(new CompositeKey(tp6.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp2.getStringRepresentation(),
		Position.SUBJECT));

	assertQueue(expected, queue);

	// Predicate test
	actual = new CompositeKey(tp6.getStringRepresentation(),
		Position.PREDICATE);
	queue.clear();
	BGPVerifyUtil.addDependingFieldsToQueue(actual, queue, bgp);

	expected.clear();
	expected.add(new CompositeKey(tp2.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp1.getStringRepresentation(),
		Position.OBJECT));

	assertQueue(expected, queue);

	// Self loop test

	actual = new CompositeKey(tp8.getStringRepresentation(),
		Position.SUBJECT);
	queue.clear();
	BGPVerifyUtil.addDependingFieldsToQueue(actual, queue, bgp);

	expected.clear();
	expected.add(new CompositeKey(tp1.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp3.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp4.getStringRepresentation(),
		Position.SUBJECT));

	assertQueue(expected, queue);

	// Predicate non variable

	actual = new CompositeKey(tp3.getStringRepresentation(),
		Position.PREDICATE);
	queue.clear();
	BGPVerifyUtil.addDependingFieldsToQueue(actual, queue, bgp);

	expected.clear();
	expected.add(new CompositeKey(tp1.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp4.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp8.getStringRepresentation(),
		Position.SUBJECT));
	expected.add(new CompositeKey(tp8.getStringRepresentation(),
		Position.OBJECT));
	expected.add(new CompositeKey(tp2.getStringRepresentation(),
		Position.OBJECT));

	assertQueue(expected, queue);
    }

    @Test
    public void getJoinCompositeKeys() {
	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	TriplePattern tp11 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp21 = new TriplePattern("?a", "likes", "?b");
	TriplePattern tp31 = new TriplePattern("?a", "knows", "?c");
	TriplePattern tp41 = new TriplePattern("?t", "knows", "X");
	TriplePattern tp51 = new TriplePattern("?b", "?t", "Y");
	TriplePattern tp61 = new TriplePattern("?c", "knows", "?a");
	TriplePattern tp71 = new TriplePattern("?t", "knows", "?b");

	bgp.add(tp11);
	bgp.add(tp21);
	bgp.add(tp31);
	bgp.add(tp41);
	bgp.add(tp51);
	bgp.add(tp61);
	bgp.add(tp71);
	// Test empty result
	CompositeKey key = new CompositeKey(tp41.getStringRepresentation(),
		Position.SUBJECT);

	Set<CompositeKey> result = BGPVerifyUtil.getJoinCompositeKeys(key, bgp);

	Set<CompositeKey> expected = new HashSet<>();

	assertEquals(expected, result);

	// Test object object join
	key = new CompositeKey(tp11.getStringRepresentation(), Position.SUBJECT);

	result = BGPVerifyUtil.getJoinCompositeKeys(key, bgp);
	expected.clear();
	expected.add(new CompositeKey(tp21.getStringRepresentation(),
		Position.SUBJECT));

	assertEquals(expected, result);

	// Loop
	key = new CompositeKey(tp31.getStringRepresentation(), Position.OBJECT);

	result = BGPVerifyUtil.getJoinCompositeKeys(key, bgp);
	expected.clear();
	expected.add(new CompositeKey(tp61.getStringRepresentation(),
		Position.SUBJECT));

	assertEquals(expected, result);

	// Predicate variable
	key = new CompositeKey(tp51.getStringRepresentation(),
		Position.PREDICATE);

	result = BGPVerifyUtil.getJoinCompositeKeys(key, bgp);
	expected.clear();
	expected.add(new CompositeKey(tp71.getStringRepresentation(),
		Position.SUBJECT));

	assertEquals(expected, result);
    }

    @Test
    public void checkTriplePatternInstantiation() {
	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	TriplePattern tp11 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp21 = new TriplePattern("?a", "likes", "?b");
	TriplePattern tp31 = new TriplePattern("?a", "knows", "?c");
	TriplePattern tp41 = new TriplePattern("?t", "knows", "X");
	TriplePattern tp51 = new TriplePattern("?b", "?t", "Y");
	TriplePattern tp61 = new TriplePattern("?c", "knows", "?a");
	TriplePattern tp71 = new TriplePattern("?t", "knows", "?b");

	bgp.add(tp11);
	bgp.add(tp21);
	bgp.add(tp31);
	bgp.add(tp41);
	bgp.add(tp51);
	bgp.add(tp61);
	bgp.add(tp71);

	// Test: Delete one subject
	CompositeKey key = new CompositeKey(tp1.getStringRepresentation(),
		Position.SUBJECT);
	Map<CompositeKey, List<ResultValue>> nodeResults = new HashMap<>();
	List<ResultValue> tpRes = new ArrayList<>();
	tpRes.add(new ResultValue("knows", "Peter"));
	tpRes.add(new ResultValue("knows", "NonExisting"));
	nodeResults.put(key, tpRes);

	List<ResultValue> tpRes2 = new ArrayList<>();
	tpRes2.add(new ResultValue("likes", "Peter"));
	nodeResults.put(new CompositeKey(tp21.getStringRepresentation(),
		Position.SUBJECT), tpRes2);

	Map<CompositeKey, List<ResultValue>> expected = new HashMap<>();
	List<ResultValue> expectedRes1 = new ArrayList<>();
	expectedRes1.add(new ResultValue("knows", "Peter"));

	expected.put(key, expectedRes1);
	expected.put(new CompositeKey(tp21.getStringRepresentation(),
		Position.SUBJECT), tpRes2);

	BGPVerifyUtil.checkTriplePatternInstantiation(key, nodeResults, bgp,
		"Hans");

	assertEquals(expected, nodeResults);

	// Delete one object with loop
	nodeResults.clear();
	tpRes.clear();

	key = new CompositeKey(tp31.getStringRepresentation(), Position.OBJECT);
	tpRes.add(new ResultValue("knows", "Peter"));
	tpRes.add(new ResultValue("knows", "NonExisting"));

	nodeResults.put(key, tpRes);

	tpRes2.clear();
	tpRes2.add(new ResultValue("knows", "Peter"));
	nodeResults.put(new CompositeKey(tp61.getStringRepresentation(),
		Position.SUBJECT), tpRes2);

	BGPVerifyUtil.checkTriplePatternInstantiation(key, nodeResults, bgp,
		"Hans");

	expected.clear();
	expectedRes1.clear();
	expectedRes1.add(new ResultValue("knows", "Peter"));

	expected.put(key, expectedRes1);
	expected.put(new CompositeKey(tp61.getStringRepresentation(),
		Position.SUBJECT), tpRes2);

	assertEquals(expected, nodeResults);

	// Test variable predicate
	nodeResults.clear();
	tpRes.clear();

	key = new CompositeKey(tp51.getStringRepresentation(),
		Position.PREDICATE);
	tpRes.add(new ResultValue("Hans", "Y"));

	nodeResults.put(key, tpRes);

	tpRes2.clear();
	tpRes2.add(new ResultValue("knows", "Hans"));
	nodeResults.put(new CompositeKey(tp71.getStringRepresentation(),
		Position.SUBJECT), tpRes2);

	expected.clear();
	expectedRes1.clear();
	expectedRes1.add(new ResultValue("Hans", "Y"));
	expected.put(key, expectedRes1);
	List<ResultValue> expectedRes2 = new ArrayList<>();
	expectedRes2.add(new ResultValue("knows", "Hans"));

	expected.put(new CompositeKey(tp71.getStringRepresentation(),
		Position.SUBJECT), expectedRes2);

	BGPVerifyUtil.checkTriplePatternInstantiation(key, nodeResults, bgp,
		"variablePred");
	assertEquals(expected, nodeResults);
    }

    @Test
    public void getShortcuts() {
	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	TriplePattern tp11 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp21 = new TriplePattern("?a", "knows", "?c");
	TriplePattern tp31 = new TriplePattern("?b", "knows", "?c");
	TriplePattern tp41 = new TriplePattern("?b", "?t", "Y");
	TriplePattern tp51 = new TriplePattern("?c", "knows", "?a");

	bgp.add(tp11);
	bgp.add(tp21);
	bgp.add(tp31);
	bgp.add(tp41);
	bgp.add(tp51);

	// Test subject
	Set<Shortcut> expected = new HashSet<>();
	expected.add(new Shortcut(new CompositeKey(tp21
		.getStringRepresentation(), Position.SUBJECT),
		new CompositeKey(tp31.getStringRepresentation(),
			Position.SUBJECT)));

	assertEquals(expected, BGPVerifyUtil.getShortcuts(bgp,
		new CompositeKey(tp11.getStringRepresentation(),
			Position.SUBJECT)));

	// Test object
	expected.clear();
	expected.add(new Shortcut(new CompositeKey(tp31
		.getStringRepresentation(), Position.OBJECT), new CompositeKey(
		tp11.getStringRepresentation(), Position.SUBJECT)));

	assertEquals(expected, BGPVerifyUtil.getShortcuts(bgp,
		new CompositeKey(tp21.getStringRepresentation(),
			Position.OBJECT)));

	// Test object subject
	expected.clear();
	expected.add(new Shortcut(new CompositeKey(tp31
		.getStringRepresentation(), Position.SUBJECT),
		new CompositeKey(tp21.getStringRepresentation(),
			Position.SUBJECT)));

	assertEquals(expected, BGPVerifyUtil.getShortcuts(bgp,
		new CompositeKey(tp11.getStringRepresentation(),
			Position.OBJECT)));
    }

    @Test
    public void checkShortcuts() {
	List<TriplePattern> bgp = new ArrayList<TriplePattern>();
	TriplePattern tp11 = new TriplePattern("?a", "knows", "?b");
	TriplePattern tp21 = new TriplePattern("?a", "knows", "?c");
	TriplePattern tp31 = new TriplePattern("?b", "knows", "?c");
	TriplePattern tp41 = new TriplePattern("?b", "?t", "Y");
	TriplePattern tp51 = new TriplePattern("?c", "knows", "?a");

	bgp.add(tp11);
	bgp.add(tp21);
	bgp.add(tp31);
	bgp.add(tp41);
	bgp.add(tp51);

	// Test: Two shortcuts
	CompositeKey key = new CompositeKey(tp11.getStringRepresentation(),
		Position.SUBJECT);
	Map<CompositeKey, List<ResultValue>> nodeResults = new HashMap<>();
	List<ResultValue> tpRes1 = new ArrayList<>();
	tpRes1.add(new ResultValue("knows", "b1"));
	tpRes1.add(new ResultValue("knows", "b2"));
	nodeResults.put(key, tpRes1);

	List<ResultValue> tpRes2 = new ArrayList<>();
	tpRes2.add(new ResultValue("knows", "c1"));
	tpRes2.add(new ResultValue("knows", "c2"));
	nodeResults.put(new CompositeKey(tp21.getStringRepresentation(),
		Position.SUBJECT), tpRes2);

	// Child results
	Map<CompositeKeyNodeAttr, List<ResultValue>> childResults = new HashMap<>();
	List<ResultValue> childRes = new ArrayList<>();
	childRes.add(new ResultValue("knows", "c1"));
	childResults.put(
		new CompositeKeyNodeAttr(tp31.getStringRepresentation(),
			Position.SUBJECT, "b1"), childRes);

	// Parent results
	Map<CompositeKeyNodeAttr, List<ResultValue>> parentResults = new HashMap<>();

	// Expected
	Map<CompositeKey, List<ResultValue>> expected = new HashMap<>();
	List<ResultValue> ex1 = new ArrayList<>();
	ex1.add(new ResultValue("knows", "b1"));

	expected.put(key, ex1);

	List<ResultValue> ex2 = new ArrayList<>();
	ex2.add(new ResultValue("knows", "c1"));

	expected.put(new CompositeKey(tp21.getStringRepresentation(),
		Position.SUBJECT), ex2);

	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp11.getStringRepresentation(),
			Position.SUBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");
	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp21.getStringRepresentation(),
			Position.SUBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");

	assertEquals(expected, nodeResults);

	// Test again: Nothing should change
	BGPVerifyUtil.checkShortcuts(key, nodeResults, childResults,
		parentResults, bgp, "Hans");
	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp21.getStringRepresentation(),
			Position.SUBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");

	assertEquals(expected, nodeResults);

	// Test 2
	nodeResults.clear();
	tpRes1.clear();
	tpRes2.clear();

	tpRes1.add(new ResultValue("knows", "a1"));
	tpRes1.add(new ResultValue("knows", "a2"));

	nodeResults.put(new CompositeKey(tp11.getStringRepresentation(),
		Position.OBJECT), tpRes1);

	tpRes2.add(new ResultValue("knows", "c1"));
	tpRes2.add(new ResultValue("knows", "c2"));

	nodeResults.put(new CompositeKey(tp31.getStringRepresentation(),
		Position.SUBJECT), tpRes2);

	// Parent results
	parentResults = new HashMap<>();
	List<ResultValue> parentRes = new ArrayList<>();
	parentRes.add(new ResultValue("knows", "c1"));
	parentResults.put(
		new CompositeKeyNodeAttr(tp21.getStringRepresentation(),
			Position.SUBJECT, "a1"), parentRes);

	// Expected
	expected.clear();
	ex1.clear();
	childResults.clear();
	ex1.add(new ResultValue("knows", "a1"));

	expected.put(new CompositeKey(tp11.getStringRepresentation(),
		Position.OBJECT), ex1);

	ex2.clear();
	ex2.add(new ResultValue("knows", "c1"));

	expected.put(new CompositeKey(tp31.getStringRepresentation(),
		Position.SUBJECT), ex2);

	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp11.getStringRepresentation(),
			Position.OBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");
	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp31.getStringRepresentation(),
			Position.SUBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");

	assertEquals(expected, nodeResults);

	// Test 3
	nodeResults.clear();
	childResults.clear();
	tpRes1.clear();
	tpRes2.clear();

	tpRes1.add(new ResultValue("knows", "a1"));
	tpRes1.add(new ResultValue("knows", "a2"));

	nodeResults.put(new CompositeKey(tp21.getStringRepresentation(),
		Position.OBJECT), tpRes1);

	tpRes2.add(new ResultValue("knows", "b1"));
	tpRes2.add(new ResultValue("knows", "b2"));

	nodeResults.put(new CompositeKey(tp31.getStringRepresentation(),
		Position.OBJECT), tpRes2);

	// Parent results
	parentResults = new HashMap<>();
	parentRes.clear();
	parentRes.add(new ResultValue("knows", "b1"));
	parentResults.put(
		new CompositeKeyNodeAttr(tp11.getStringRepresentation(),
			Position.SUBJECT, "a1"), parentRes);

	// Expected
	expected.clear();
	ex1.clear();
	ex1.add(new ResultValue("knows", "a1"));

	expected.put(new CompositeKey(tp21.getStringRepresentation(),
		Position.OBJECT), ex1);

	ex2.clear();
	ex2.add(new ResultValue("knows", "b1"));

	expected.put(new CompositeKey(tp31.getStringRepresentation(),
		Position.OBJECT), ex2);

	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp21.getStringRepresentation(),
			Position.OBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");
	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp31.getStringRepresentation(),
			Position.OBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");

	assertEquals(expected, nodeResults);

	// Test 4: Self loop
	nodeResults.clear();
	childResults.clear();

	TriplePattern tp61 = new TriplePattern("?a", "rdf:type", "\"user\"");
	bgp.add(tp61);

	tpRes1.clear();
	tpRes2.clear();

	tpRes1.add(new ResultValue("knows", "b1"));
	tpRes1.add(new ResultValue("knows", "b2"));
	tpRes1.add(new ResultValue("knows", "Hans"));

	nodeResults.put(new CompositeKey(tp11.getStringRepresentation(),
		Position.SUBJECT), tpRes1);

	tpRes2.add(new ResultValue("knows", "c1"));
	tpRes2.add(new ResultValue("knows", "c2"));

	nodeResults.put(new CompositeKey(tp21.getStringRepresentation(),
		Position.SUBJECT), tpRes2);

	List<ResultValue> tpRes3 = new ArrayList<>();
	tpRes3.add(new ResultValue("rdf:type", "\"user\""));
	nodeResults.put(new CompositeKey(tp61.getStringRepresentation(),
		Position.SUBJECT), tpRes3);

	// Child results
	childRes.clear();
	List<ResultValue> childRes3 = new ArrayList<>();
	childRes3.add(new ResultValue("knows", "c4"));
	childResults.put(
		new CompositeKeyNodeAttr(tp31.getStringRepresentation(),
			Position.SUBJECT, "b2"), childRes3);

	childRes.add(new ResultValue("knows", "c1"));
	childResults.put(
		new CompositeKeyNodeAttr(tp31.getStringRepresentation(),
			Position.SUBJECT, "b1"), childRes);

	List<ResultValue> childRes2 = new ArrayList<ResultValue>();
	childRes2.add(new ResultValue("knows", "Hans"));
	childResults.put(
		new CompositeKeyNodeAttr(tp11.getStringRepresentation(),
			Position.OBJECT, "Hans"), childRes2);

	// Expected
	expected.clear();
	ex1.clear();
	ex1.add(new ResultValue("knows", "b1"));

	expected.put(new CompositeKey(tp11.getStringRepresentation(),
		Position.SUBJECT), ex1);

	ex2.clear();
	ex2.add(new ResultValue("knows", "c1"));
	expected.put(new CompositeKey(tp21.getStringRepresentation(),
		Position.SUBJECT), ex2);
	expected.put(new CompositeKey(tp61.getStringRepresentation(),
		Position.SUBJECT), tpRes3);

	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp11.getStringRepresentation(),
			Position.SUBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");
	BGPVerifyUtil.checkShortcuts(
		new CompositeKey(tp21.getStringRepresentation(),
			Position.SUBJECT), nodeResults, childResults,
		parentResults, bgp, "Hans");

	assertEquals(expected, nodeResults);
    }

    /**
     * Checking that two queues have the same elements, but disregarding the
     * order
     * 
     * @param expected
     *            Expected queue
     * @param result
     *            The result queue
     */
    private <T> void assertQueue(Queue<T> expected, Queue<T> result) {
	assertEquals(expected.size(), result.size());
	for (T cp : expected) {
	    assertEquals(true, result.contains(cp));
	}
    }

}
