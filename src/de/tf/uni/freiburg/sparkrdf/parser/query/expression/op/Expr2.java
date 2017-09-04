package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.util.NodeFactoryExtra;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public abstract class Expr2 implements IExpression {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -3478857114950844403L;

    /**
     * Left expression
     */
    protected IExpression expr1;

    /**
     * Right expression
     */
    protected IExpression expr2;

    /**
     * Create a new expression with two sub expressions
     * 
     * @param left
     *            Left {@link IExpression}
     * @param right
     *            Right {@link IExpression}
     */
    public Expr2(IExpression left, IExpression right) {
	this.expr1 = left;
	this.expr2 = right;
    }

    /**
     * Compare the two stored expressions lexicographically
     * 
     * @param solution
     *            {@link SolutionMapping} to get the values for the variables
     * @return {@link String#compareTo(String)}
     */
    protected Integer compareExpressions(SolutionMapping solution) {
	String left = "";
	String right = "";
	if (expr1 instanceof ExprVar) {
	    left = solution.getValueToField(((ExprVar) expr1).getVar());
	} else if (expr1 instanceof IValueType) {
	    right = ((IValueType) expr1).getValue(solution);
	}

	if (expr2 instanceof ExprVar) {
	    right = solution.getValueToField(((ExprVar) expr2).getVar());
	} else if (expr2 instanceof IValueType) {
	    right = ((IValueType) expr2).getValue(solution);
	}

	if (isInteger(left) && isInteger(right)) {
	    return getInteger(left) - getInteger(right);
	}

	if (left == null || right == null) {
	    return null;
	}

	int res = left.compareTo(right);
	return res;
    }

    /**
     * Check if a String is an Integer
     * 
     * @param toTest
     *            String to test
     * @return True if it is an integer, false otherwise
     */
    private Boolean isInteger(String toTest) {
	if (toTest == null) {
	    return false;
	}

	try {
	    Node testNode = NodeFactoryExtra.parseNode(toTest);
	    return testNode.toString().endsWith(
		    "^^http://www.w3.org/2001/XMLSchema#integer") ? true
		    : false;
	} catch (Exception e) {
	    return false;
	}
    }

    /**
     * Get the integer out of the given String
     * 
     * @param toGet
     *            String which contains the integer
     * @return Parsed integer
     */
    private Integer getInteger(String toGet) {
	Node testNode = NodeFactoryExtra.parseNode(toGet);
	Integer intgr = (Integer) testNode.getLiteral().getValue();
	return intgr;
    }

}
