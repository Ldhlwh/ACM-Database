package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate predicate;
    private DbIterator childIt1;
    private DbIterator childIt2;
    private TupleDesc tupleDesc;
    private boolean fetched = false;
    private Map<Integer, ArrayList<Tuple>> hashed = new HashMap<>();
    private Tuple t1;
    private Iterator<Tuple> t2It;
    private boolean fetching = false; // true if an ArrayList is half-fetched
    
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        predicate = p;
        childIt1 = child1;
        childIt2 = child2;
        tupleDesc = TupleDesc.merge(childIt1.getTupleDesc(), childIt2.getTupleDesc());
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
	return childIt1.getTupleDesc().getFieldName(predicate.getField1());
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return childIt2.getTupleDesc().getFieldName(predicate.getField2());
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        childIt1.open();
        childIt2.open();
    }

    public void close() {
        // some code goes here
        childIt1.close();
        childIt2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childIt1.rewind();
        childIt2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!fetched)
        {
            fetched = true;
            if(!childIt2.hasNext())
                return null;
            while(childIt2.hasNext())
            {
                Tuple t2 = childIt2.next();
                int t2FieldHashed = t2.getField(predicate.getField2()).hashCode();
                if(!hashed.containsKey(t2FieldHashed))
                {
                    ArrayList<Tuple> al = new ArrayList<>();
                    al.add(t2);
                    hashed.put(t2FieldHashed, al);
                }
                else
                {
                    hashed.get(t2FieldHashed).add(t2);
                }
            }
        }
        
        if(fetching)
        {
            if(t2It.hasNext())
            {
                Tuple t2 = t2It.next();
                if(predicate.filter(t1, t2))
                {
                    Tuple newTuple = new Tuple(tupleDesc);
                    int now = 0;
                    Iterator<Field> it = t1.fields();
                    while(it.hasNext())
                    {
                        newTuple.setField(now++, it.next());
                    }
                    it = t2.fields();
                    while(it.hasNext())
                    {
                        newTuple.setField(now++, it.next());
                    }
                    return newTuple;
                }
            }
            fetching = false;
        }
        
        while(childIt1.hasNext())
        {
            t1 = childIt1.next();
            int t1FieldHashed = t1.getField(predicate.getField1()).hashCode();
            if(hashed.containsKey(t1FieldHashed))
            {
                ArrayList<Tuple> t2s = hashed.get(t1FieldHashed);
                t2It = t2s.iterator();
                Tuple t2 = t2It.next();
                if(predicate.filter(t1, t2))
                {
                    fetching = true;
                    Tuple newTuple = new Tuple(tupleDesc);
                    int now = 0;
                    Iterator<Field> it = t1.fields();
                    while(it.hasNext())
                    {
                        newTuple.setField(now++, it.next());
                    }
                    it = t2.fields();
                    while(it.hasNext())
                    {
                        newTuple.setField(now++, it.next());
                    }
                    return newTuple;
                }
            }
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[2];
        children[0] = childIt1;
        children[1] = childIt2;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        childIt1 = children[0];
        childIt2 = children[1];
    }
    
}
