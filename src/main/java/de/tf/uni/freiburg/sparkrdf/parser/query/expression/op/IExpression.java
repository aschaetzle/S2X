package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import java.io.Serializable;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public interface IExpression extends Serializable {

    public abstract Boolean evaluate(SolutionMapping solution);
}
