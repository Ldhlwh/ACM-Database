package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator childIt;
    private int aField;
    private int gField;
    private Aggregator.Op op;
    private Aggregator aggregator;
    private DbIterator rtnIt;
    private boolean fetched = false;
    private String gbFieldName = null;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
		childIt = child;
		aField = afield;
		gField = gfield;
		op = aop;
		Type gType;
		if(gField == Aggregator.NO_GROUPING)
		{
			gType = null;
		}
		else
		{
			gType = childIt.getTupleDesc().getFieldType(gField);
			gbFieldName = childIt.getTupleDesc().getFieldName(gField);
		}
		if(childIt.getTupleDesc().getFieldType(aField) == Type.INT_TYPE)
		{
			aggregator = new IntegerAggregator(gField, gType, aField, op);
		}
		else
		{
			aggregator = new StringAggregator(gField, gType, aField, op);
		}
		rtnIt = aggregator.iterator();
		if(gField != Aggregator.NO_GROUPING)
			rtnIt.getTupleDesc().setFieldName(0, gbFieldName);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
		// some code goes here
		return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
		if(gField != Aggregator.NO_GROUPING)
		{
			return childIt.getTupleDesc().getFieldName(gField);
		}
		return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		// some code goes here
		return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		// some code goes here
		return childIt.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		// some code goes here
		return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		// some code goes here
		super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if(!fetched)
		{
			childIt.open();
			while(childIt.hasNext())
			{
				Tuple t = childIt.next();
				aggregator.mergeTupleIntoGroup(t);
			}
			childIt.close();
			fetched = true;
			rtnIt = aggregator.iterator();
			if(gField != Aggregator.NO_GROUPING)
				rtnIt.getTupleDesc().setFieldName(0, gbFieldName);
			rtnIt.open();
		}
		while(rtnIt.hasNext())
		{
			return rtnIt.next();
		}
		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		rtnIt.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
		// some code goes here
		return rtnIt.getTupleDesc();
    }

    public void close() {
		// some code goes here
		rtnIt.close();
		super.close();
    }

    @Override
    public DbIterator[] getChildren() {
		// some code goes here
		DbIterator[] children = new DbIterator[1];
		children[0] = childIt;
		return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
		// some code goes here
		childIt = children[0];
    }
    
}