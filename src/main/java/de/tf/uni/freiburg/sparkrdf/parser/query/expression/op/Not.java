package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class Not extends Expr1 {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -5560134478604586247L;

    public Not(IExpression expr1) {
	super(expr1);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	return !expr1.evaluate(solution);
    }

}
