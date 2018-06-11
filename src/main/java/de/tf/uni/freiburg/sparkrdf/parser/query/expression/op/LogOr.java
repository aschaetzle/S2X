package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class LogOr extends Expr2 {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -4440095423842139887L;

    public LogOr(IExpression left, IExpression right) {
	super(left, right);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	if (compareExpressions(solution) == null) {
	    return false;
	}
	return expr1.evaluate(solution) || expr2.evaluate(solution);
    }

}
