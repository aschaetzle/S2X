package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class NotEquals extends Expr2 {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = 8292702113993295416L;

    public NotEquals(IExpression left, IExpression right) {
	super(left, right);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	if (compareExpressions(solution) == null) {
	    return false;
	}
	return (compareExpressions(solution) != 0) ? true : false;
    }

}
