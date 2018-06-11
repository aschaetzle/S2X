package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class Lang extends Expr1 implements IValueType {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = 208593033367133594L;

    public Lang(IExpression expr1) {
	super(expr1);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	throw new UnsupportedOperationException();
    }

    @Override
    public String getValue(SolutionMapping solution) {
	String lang = "";

	if (expr1 instanceof NodeValue) {
	    lang = ((NodeValue) expr1).getValue(solution);
	}

	if (expr1 instanceof ExprVar) {
	    lang = solution.getValueToField(((ExprVar) expr1).getVar());
	}
	return lang;
    }
}
