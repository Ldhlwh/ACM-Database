package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator childIt;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean fetched = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        tid = t;
        childIt = child;
        this.tableId = tableId;
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        tupleDesc = new TupleDesc(typeAr);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        childIt.open();
    }

    public void close() {
        // some code goes here
        childIt.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childIt.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(fetched)
            return null;
        fetched = true;
        int cnt = 0;
        while(childIt.hasNext())
        {
            Tuple t = childIt.next();
            try
            {
                Database.getBufferPool().insertTuple(tid, tableId, t);
                cnt++;
            }
            catch(Exception e)
            {
                System.err.println("A tuple is failed to be inserted.");
            }
        }
        Tuple rtn = new Tuple(tupleDesc);
        rtn.setField(0, new IntField(cnt));
        return rtn;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[1];
        children[0] = childIt;
        return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        childIt = children[0];
    }
}
