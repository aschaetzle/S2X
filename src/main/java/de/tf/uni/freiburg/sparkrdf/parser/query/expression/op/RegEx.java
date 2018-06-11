package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class RegEx extends Expr2 {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -4408622036509672641L;

    public RegEx(IExpression left, IExpression right) {
	super(left, right);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	// Only FILTER regex(?a, "ex:user") supported
	String left = "";
	String right = "";
	if (expr1 instanceof ExprVar) {
	    left = solution.getValueToField(((ExprVar) expr1).getVar());
	} else if (expr1 instanceof NodeValue) {
	    left = ((NodeValue) expr1).getValue(solution);
	}

	if (expr2 instanceof ExprVar) {
	    right = solution.getValueToField(((ExprVar) expr2).getVar());
	} else if (expr2 instanceof NodeValue) {
	    right = ((NodeValue) expr2).getValue(solution);
	}

	if (left == null || right == null) {
	    return false;
	}

	// Remove trailing and leading quotes
	right = right.substring(1, right.length() - 1);

	return left.contains(right);
    }

}
