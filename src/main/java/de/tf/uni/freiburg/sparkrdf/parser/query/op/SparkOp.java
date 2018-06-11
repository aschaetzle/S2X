package de.tf.uni.freiburg.sparkrdf.parser.query.op;

/**
 * Interface for all operators.
 * 
 * @author Thorsten Berberich
 * 
 */
public interface SparkOp {

    /**
     * Execute the operator
     */
    public void execute();

    /**
     * Get the tag of the operator
     * 
     * @return Tag of operator
     */
    public String getTag();
}
