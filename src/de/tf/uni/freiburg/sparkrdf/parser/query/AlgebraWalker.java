package de.tf.uni.freiburg.sparkrdf.parser.query;

import java.util.Iterator;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitor;
import com.hp.hpl.jena.sparql.algebra.OpVisitorByType;
import com.hp.hpl.jena.sparql.algebra.op.Op0;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpN;

/**
 * @author Thorsten Berberich
 */
public class AlgebraWalker extends OpVisitorByType {

    private OpVisitor visitor;

    public AlgebraWalker(OpVisitor visitor) {
	this.visitor = visitor;
    }

    @Override
    protected void visitN(OpN op) {
	for (Iterator<Op> iter = op.iterator(); iter.hasNext();) {
	    Op sub = iter.next();
	    sub.visit(this);
	}
	op.visit(visitor);
    }

    @Override
    protected void visit2(Op2 op) {
	if (op.getLeft() != null)
	    op.getLeft().visit(this);
	if (op.getRight() != null)
	    op.getRight().visit(this);
	op.visit(visitor);
    }

    @Override
    protected void visit1(Op1 op) {
	if (op.getSubOp() != null)
	    op.getSubOp().visit(this);
	op.visit(visitor);
    }

    @Override
    protected void visit0(Op0 op) {
	op.visit(visitor);
    }

    @Override
    protected void visitExt(OpExt op) {
	op.visit(visitor);
    }

    @Override
    protected void visitFilter(OpFilter op) {
	if (op.getSubOp() != null)
	    op.getSubOp().visit(this);
	op.visit(visitor);
    }

    @Override
    protected void visitLeftJoin(OpLeftJoin op) {
	if (op.getLeft() != null)
	    op.getLeft().visit(this);
	if (op.getRight() != null)
	    op.getRight().visit(this);
	op.visit(visitor);
    }

}
