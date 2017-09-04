package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class Bound extends Expr1 {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -4111412284098612993L;

    public Bound(IExpression expr1) {
	super(expr1);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	String varName = ((ExprVar) expr1).getVar();
	String mapping = solution.getValueToField(varName);
	return (mapping != null) ? true : false;
    }
}
