package de.tf.uni.freiburg.sparkrdf.parser.query;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpConditional;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpReduced;
import com.hp.hpl.jena.sparql.algebra.op.OpSequence;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.expr.ExprList;

import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkBGP;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkDistinct;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkFilter;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkLeftJoin;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkOp;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkOrderBy;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkProjection;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkSlice;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkUnion;

/**
 * @author Thorsten Berberich
 */
public class AlgebraTranslator extends OpVisitorBase {

    /**
     * All prefixes
     */
    private final PrefixMapping prefixes;

    /**
     * All operators to execute
     */
    private Queue<SparkOp> executionQueue = new LinkedList<>();

    /**
     * Create a new translator
     * 
     * @param prefixes
     *            All prefixes to use
     */
    public AlgebraTranslator(PrefixMapping prefixes) {
	this.prefixes = prefixes;
    }

    /**
     * Get the queue with the {@link SparkOp}s
     * 
     * @return Queue of {@link SparkOp}s
     */
    public Queue<SparkOp> getExecutionQueue() {
	return executionQueue;
    }

    @Override
    public void visit(OpBGP opBGP) {
	SparkBGP bgp = new SparkBGP(opBGP, prefixes);
	/*
	 * Add it twice. The first run will match the BGP and the second run
	 * will build the result. This is done for the exact time measuring and
	 * could basically done with one object but then without exact time
	 * measuring.
	 */
	executionQueue.add(bgp);
	executionQueue.add(bgp);
    }

    @Override
    public void visit(OpFilter opFilter) {
	executionQueue.add(new SparkFilter(opFilter, prefixes));
	addExpressionsToBGP(opFilter.getExprs());
    }

    @Override
    public void visit(OpJoin opJoin) {
    }

    @Override
    public void visit(OpSequence opSequence) {
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
	executionQueue.add(new SparkLeftJoin(opLeftJoin, prefixes));
	if (opLeftJoin.getExprs() != null) {
	    addExpressionsToBGPLeftJoin(opLeftJoin.getExprs());
	}
    }

    @Override
    public void visit(OpConditional opConditional) {
    }

    @Override
    public void visit(OpUnion opUnion) {
	executionQueue.add(new SparkUnion(opUnion));
    }

    @Override
    public void visit(OpProject opProject) {
	executionQueue.add(new SparkProjection(opProject));
    }

    @Override
    public void visit(OpDistinct opDistinct) {
	executionQueue.add(new SparkDistinct(opDistinct));
    }

    @Override
    public void visit(OpOrder opOrder) {
	executionQueue.add(new SparkOrderBy(opOrder, prefixes));
    }

    @Override
    public void visit(OpSlice opSlice) {
	executionQueue.add(new SparkSlice(opSlice));
    }

    @Override
    public void visit(OpReduced opReduced) {
    }

    /**
     * Put an expression into an Basic Graph Pattern if it can be executed
     * directly
     * 
     * @param expr
     *            Expressions that should be added
     */
    private void addExpressionsToBGP(ExprList expr) {
	Stack<SparkOp> stack = new Stack<SparkOp>();

	while (!executionQueue.isEmpty()) {
	    stack.push(executionQueue.poll());
	}

	int distance = 0;
	Boolean added = false;

	Stack<SparkOp> stack2 = new Stack<SparkOp>();

	while (!stack.isEmpty()) {
	    SparkOp actual = stack.pop();
	    stack2.push(actual);

	    // Filter is directly next to the BGP
	    if (actual instanceof SparkBGP && distance == 1) {
		((SparkBGP) actual).addExpressions(expr);
		added = true;
	    }
	    distance = distance + 1;
	}

	int itr = 0;
	while (!stack2.isEmpty()) {
	    SparkOp actual = stack2.pop();
	    executionQueue.add(actual);
	    if (!added && itr == 0 && actual instanceof SparkBGP) {
		((SparkBGP) actual).addExpressions(expr);
		itr++;
	    }
	}
    }

    /**
     * Add an expression to a left join if it can be executed with it
     * 
     * @param expr
     *            Expressions to execute
     */
    private void addExpressionsToBGPLeftJoin(ExprList expr) {
	Stack<SparkOp> stack = new Stack<SparkOp>();

	while (!executionQueue.isEmpty()) {
	    stack.push(executionQueue.poll());
	}

	Stack<SparkOp> stack2 = new Stack<SparkOp>();
	int distance = 0;

	while (!stack.isEmpty()) {
	    SparkOp actual = stack.pop();
	    stack2.push(actual);

	    if (actual instanceof SparkBGP && distance == 1) {
		((SparkBGP) actual).addExpressions(expr);
	    }
	    distance = distance + 1;
	}

	while (!stack2.isEmpty()) {
	    SparkOp actual = stack2.pop();
	    executionQueue.add(actual);
	}
    }
}
