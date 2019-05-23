package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Map<Field, Integer> fieldToCnt = new HashMap<>();
    private int cnt = 0;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        if(what != Op.COUNT)
            throw new IllegalArgumentException("String fields do not support operators other than COUNT.");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbField == NO_GROUPING)
        {
            cnt++;
        }
        else
        {
            Field group = tup.getField(gbField);
            if(!fieldToCnt.containsKey(group))
            {
                fieldToCnt.put(group, 1);
            }
            else
            {
                fieldToCnt.put(group, fieldToCnt.get(group) + 1);
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        if(gbField == NO_GROUPING)
        {
            Type[] typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            String[] fieldAr = new String[1];
            fieldAr[0] = "aggregateVal";
            TupleDesc rtnDesc = new TupleDesc(typeAr, fieldAr);
            ArrayList<Tuple> rtn = new ArrayList<>();
            Tuple t = new Tuple(rtnDesc);
            t.setField(0, new IntField(cnt));
            rtn.add(t);
            return new TupleIterator(rtnDesc, rtn);
        }
        else
        {
            Type[] typeAr = new Type[2];
            typeAr[0] = gbFieldType;
            typeAr[1] = Type.INT_TYPE;
            String[] fieldAr = new String[2];
            fieldAr[0] = "groupValue";
            fieldAr[1] = "aggregateVal";
            TupleDesc rtnDesc = new TupleDesc(typeAr, fieldAr);
            ArrayList<Tuple> rtn = new ArrayList<>();
            for(Field group : fieldToCnt.keySet())
            {
                Tuple t = new Tuple(rtnDesc);
                int curCnt = fieldToCnt.get(group);
                if(gbFieldType == Type.INT_TYPE)
                {
                    t.setField(0, new IntField(((IntField)group).getValue()));
                }
                else if(gbFieldType == Type.STRING_TYPE)
                {
                    String str = ((StringField)group).getValue();
                    t.setField(0, new StringField(str, Type.STRING_LEN));
                }
                t.setField(1, new IntField(curCnt));
                rtn.add(t);
            }
            return new TupleIterator(rtnDesc, rtn);
        }
    }

}
