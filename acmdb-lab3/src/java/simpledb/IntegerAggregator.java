package simpledb;

import com.sun.media.sound.SF2GlobalRegion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;
    private Map<Field, Integer> fieldToValue = new HashMap<>();
    private Map<Field, Integer> fieldToCnt = new HashMap<>();
    private int value, cnt = 0;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aField = afield;
        op = what;
        if(gbField == NO_GROUPING)
        {
            switch(op)
            {
                case MIN:
                    value = Integer.MAX_VALUE;
                    break;
                case MAX:
                    value = Integer.MIN_VALUE;
                    break;
                default:
                    value = 0;
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbField == NO_GROUPING)
        {
            int tupField = ((IntField)tup.getField(aField)).getValue();
            cnt++;
            switch(op)
            {
                case MIN:
                    if(tupField < value)
                        value = tupField;
                    break;
                case MAX:
                    if(tupField > value)
                        value = tupField;
                    break;
                default:
                    value += tupField;
            }
        }
        else
        {
            Field group = tup.getField(gbField);
            int tupField = ((IntField)tup.getField(aField)).getValue();
            if(!fieldToValue.containsKey(group))
            {
                fieldToValue.put(group, tupField);
                fieldToCnt.put(group, 1);
            }
            else
            {
                fieldToCnt.put(group, fieldToCnt.get(group) + 1);
                int curValue = fieldToValue.get(group);
                switch(op)
                {
                    case MIN:
                        if(tupField < curValue)
                            fieldToValue.put(group, tupField);
                        break;
                    case MAX:
                        if(tupField > curValue)
                            fieldToValue.put(group, tupField);
                        break;
                    default:
                        fieldToValue.put(group, tupField + curValue);
                }
            }
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
            switch(op)
            {
                case AVG:
                    if(cnt == 0)
                        t.setField(0, new IntField(0));
                    else
                        t.setField(0, new IntField(value / cnt));
                    break;
                case COUNT:
                    t.setField(0, new IntField(cnt));
                    break;
                default:
                    t.setField(0, new IntField(value));
            }
            rtn.add(t);
            return new TupleIterator(rtnDesc, rtn);
        }
        else
        {
            Type[] typeAr = new Type[2];
            typeAr[0] = gbFieldType;
            typeAr[1] = Type.INT_TYPE;
            String[] fieldAr = new String[2];
            fieldAr[0] = "groupVal";
            fieldAr[1] = "aggregateVal";
            TupleDesc rtnDesc = new TupleDesc(typeAr, fieldAr);
            ArrayList<Tuple> rtn = new ArrayList<>();
            for(Field group : fieldToValue.keySet())
            {
                Tuple t = new Tuple(rtnDesc);
                int curValue = fieldToValue.get(group);
                int curCnt = fieldToCnt.get(group);
                if(gbFieldType == Type.INT_TYPE)
                {
                    t.setField(0, new IntField(((IntField)group).getValue()));
                }
                else if(gbFieldType == Type.STRING_TYPE)
                {
                    String str = ((StringField)group).getValue();
                    t.setField(0, new StringField(str, str.length()));
                }
                switch(op)
                {
                    case AVG:
                        t.setField(1, new IntField(curValue / curCnt));
                        break;
                    case COUNT:
                        t.setField(1, new IntField(curCnt));
                        break;
                    default:
                        t.setField(1, new IntField(curValue));
                }
                rtn.add(t);
            }
            return new TupleIterator(rtnDesc, rtn);
        }
    }

}
