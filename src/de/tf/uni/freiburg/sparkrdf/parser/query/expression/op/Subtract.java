package de.tf.uni.freiburg.sparkrdf.parser.query.expression.op;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.util.NodeFactoryExtra;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

/**
 * @author Thorsten Berberich
 */
public class Subtract extends Expr2 implements IValueType {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = 8472666104683413108L;

    public Subtract(IExpression left, IExpression right) {
	super(left, right);
    }

    @Override
    public Boolean evaluate(SolutionMapping solution) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public String getValue(SolutionMapping solution) {
	String left = "";
	String right = "";
	if (expr1 instanceof ExprVar) {
	    left = solution.getValueToField(((ExprVar) expr1).getVar());
	} else {
	    left = ((IValueType) expr1).getValue(solution);
	}

	if (expr2 instanceof ExprVar) {
	    right = solution.getValueToField(((ExprVar) expr2).getVar());
	} else {
	    right = ((IValueType) expr2).getValue(solution);
	}

	Node nodeLeft = NodeFactoryExtra.parseNode(left);
	Node nodeRight = NodeFactoryExtra.parseNode(right);
	Integer leftInt = (Integer) nodeLeft.getLiteral().getValue();
	Integer rightInt = (Integer) nodeRight.getLiteral().getValue();

	int res = leftInt - rightInt;

	return String.format(
		"\"%s\"^^<http://www.w3.org/2001/XMLSchema#integer>", res);
    }

}
