package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class GreaterThenEqual extends Expr2 {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = 2019324738511796047L;

    public GreaterThenEqual(IExpression left, IExpression right) {
	super(left, right);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	if (compareExpressions(solution) == null) {
	    return false;
	}
	int res = compareExpressions(solution);
	return (res > 0 || res == 0) ? true : false;
    }

}
