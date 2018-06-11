package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class NodeValue implements IExpression, IValueType {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = 7708287830737418559L;
    private final String value;

    public NodeValue(String value) {
	this.value = value;
    }

    @Override
    public String getValue(SolutionMapping solution) {
	return value;
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	throw new UnsupportedOperationException();
    }
}
