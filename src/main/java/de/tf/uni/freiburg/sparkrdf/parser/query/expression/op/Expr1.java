package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

/**
 * @author Thorsten Berberich
 */
public abstract class Expr1 implements IExpression {

    /**
     * Generated ID
     */
    private static final long serialVersionUID = 6131309222914257986L;

    /**
     * Subexpression
     */
    protected final IExpression expr1;

    /**
     * Store the subexpression
     * 
     * @param expr1
     *            Subexpression
     */
    public Expr1(IExpression expr1) {
	this.expr1 = expr1;
    }

}
