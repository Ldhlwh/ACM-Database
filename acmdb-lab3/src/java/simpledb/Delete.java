package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId tid;
    private DbIterator childIt;
    private TupleDesc tupleDesc;
    private boolean fetched = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        tid = t;
        childIt = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
                Database.getBufferPool().deleteTuple(tid, t);
                cnt++;
            }
            catch(IOException e)
            {
                System.err.println("A tuple is failed to be deleted.");
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
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        childIt = children[0];
    }

}
