package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] bucketCnt;
    private int min, max;
    private double denominator;
    private int totalCnt = 0;
    private int numBuckets;
    
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        numBuckets = Math.min(buckets, max - min + 1);
        bucketCnt = new int[numBuckets];
        this.min = min;
        this.max = max;
        denominator = (max - min + 1) / (double)numBuckets;
    }
    /**
     * From now on value v should be counted into the
     * (v - min) / ((max - min + 1) / # bucket) -th bucket.
     * And to save calculation time, (max - min + 1) / # bucket is pre-calculated as above.
     */

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucketNo = (int)((v - min) / denominator);
        bucketCnt[bucketNo]++;
        totalCnt++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        if(v > max)
        {
            switch(op)
            {
                case EQUALS:
                    return 0.0;
                case NOT_EQUALS:
                    return 1.0;
                case GREATER_THAN:
                    return 0.0;
                case GREATER_THAN_OR_EQ:
                    return 0.0;
                case LESS_THAN:
                    return 1.0;
                case LESS_THAN_OR_EQ:
                    return 1.0;
                case LIKE:
                    return 0.0;
            }
            return -1.0;
        }
        if(v < min)
        {
            switch(op)
            {
                case EQUALS:
                    return 0.0;
                case NOT_EQUALS:
                    return 1.0;
                case GREATER_THAN:
                    return 1.0;
                case GREATER_THAN_OR_EQ:
                    return 1.0;
                case LESS_THAN:
                    return 0.0;
                case LESS_THAN_OR_EQ:
                    return 0.0;
                case LIKE:
                    return 0.0;
            }
            return -1.0;
        }
        int bucketNo = (int)((v - min) / denominator);
        int bucketMin = (int)Math.ceil(denominator * bucketNo) + min;
        int bucketMax = (int)Math.ceil(denominator * (bucketNo + 1)) + min - 1;
        double cnt = bucketCnt[bucketNo];
        if(op == Predicate.Op.EQUALS)
            return (cnt / (bucketMax - bucketMin + 1)) / totalCnt;
        if(op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ)
        {
            double rtn = 0.0;
            if(op == Predicate.Op.GREATER_THAN_OR_EQ)
                rtn += cnt / (bucketMax - bucketMin + 1);
            rtn += ((double)(bucketMax - v) / (bucketMax - bucketMin + 1)) * cnt;
            for(int i = bucketNo + 1; i < numBuckets; i++)
            {
                rtn += bucketCnt[i];
            }
            return rtn / totalCnt;
        }
        if(op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ)
        {
            double rtn = 0.0;
            if(op == Predicate.Op.LESS_THAN_OR_EQ)
                rtn += cnt / (bucketMax - bucketMin + 1);
            rtn += ((double)(v - bucketMin) / (bucketMax - bucketMin + 1)) * cnt;
            for(int i = bucketNo - 1; i >= 0; i--)
            {
                rtn += bucketCnt[i];
            }
            return rtn / totalCnt;
        }
        if(op == Predicate.Op.NOT_EQUALS)
        {
            return 1.0 - (cnt / (bucketMax - bucketMin + 1)) / totalCnt;
        }
        if(op == Predicate.Op.LIKE)
            return 0.1;
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuffer rtn = new StringBuffer();
        rtn.append("[");
        for(int i = 0; i < numBuckets; i++)
        {
            int bucketMin = (int)Math.ceil(denominator * i);
            int bucketMax = (int)Math.floor(denominator * (i + 1));
            rtn.append(bucketCnt[i] + "(" + bucketMin + ", " + bucketMax + ")");
            if(i != numBuckets - 1)
                rtn.append(", ");
        }
        rtn.append("]");
        return rtn.toString();
    }
}
