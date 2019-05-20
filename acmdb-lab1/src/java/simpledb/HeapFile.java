package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) throws IOException
    {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid)
            throws IllegalArgumentException, IOException
    {
        // some code goes here
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        raf.seek(pid.pageNumber() * BufferPool.getPageSize());
        byte[] data = new byte[BufferPool.getPageSize()];
        raf.read(data);
        raf.close();
        return new HeapPage(new HeapPageId(getId(), pid.pageNumber()), data);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    
    public class HeapFileIterator implements DbFileIterator {
        
        private ArrayList<Iterator<Tuple>> tupleIterators = new ArrayList<>();
        private int curIterator = 0;
        private int numIterators;
        private TransactionId tid;
        
        public HeapFileIterator(TransactionId id)
        {
            tid = id;
        }
    
        @Override
        public void open()
                throws DbException, TransactionAbortedException, IOException
        {
            numIterators = numPages();
            for(int i = 0; i < numIterators; i++)
            {
                HeapPage now = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
                tupleIterators.add(now.iterator());
            }
        }
        
        public boolean hasNext()
        {
            if(numIterators == 0)
                return false;
            for(; curIterator < numIterators; curIterator++)
            {
                if(tupleIterators.get(curIterator).hasNext())
                    return true;
            }
            return false;
        }
        
        public Tuple next()
        {
            if(!hasNext())
                throw new NoSuchElementException();
            return tupleIterators.get(curIterator).next();
        }
        
        public void remove() throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException();
        }
    
        @Override
        public void rewind()
                throws DbException, TransactionAbortedException, IOException
        {
            close();
            open();
        }
    
        @Override
        public void close()
        {
            tupleIterators.clear();
            curIterator = numIterators = 0;
        }
    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

