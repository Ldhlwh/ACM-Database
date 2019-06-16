package simpledb;

import com.sun.org.apache.bcel.internal.generic.ALOAD;

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
    public HeapFile(File f, TupleDesc td)
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
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid)
            throws IllegalArgumentException
    {
        // some code goes here
        try
        {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(pid.pageNumber() * BufferPool.getPageSize());
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.read(data);
            raf.close();
            return new HeapPage(new HeapPageId(getId(), pid.pageNumber()), data);
        }
        catch(IOException e)
        {
            System.err.println("Read Page Failed.");
            return null;
        }
    }

    // see DbFile.java for javadocs
    public synchronized void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        int pageNo = page.getId().pageNumber();
        raf.seek(pageNo * BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(file.length() / BufferPool.getPageSize());
    }
    
    private int cnt = 0;
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int numPage = numPages();
        ArrayList<Page> rtn = new ArrayList<>();
        for(int i = 0; i < numPage; i++)
        {
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            try
            {
                page.insertTuple(t);
                rtn.add(page);
                return rtn;
            }
            catch(DbException e)
            {
                if(e.getMessage().equals("TupleDesc mismatch."))
                    throw new DbException("TupleDesc mismatch, the tuple cannot be inserted.");
            }
            Database.getBufferPool().releasePage(tid, new HeapPageId(getId(), i));
        }
        // Need to create a new page to the file
        /*HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPage), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        rtn.add(newPage);
        writePage(newPage);
		*/
		HeapPage newPage = new HeapPage(new HeapPageId(getId(), numPage), HeapPage.createEmptyPageData());
		writePage(newPage);
		newPage = (HeapPage)Database.getBufferPool().getPage(tid, newPage.getId(), Permissions.READ_WRITE);
		newPage.insertTuple(t);
        rtn.add(newPage);
        return rtn;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> rtn = new ArrayList<>();
        rtn.add(page);
        return rtn;
    }
    
    public class HeapFileIterator implements DbFileIterator {
        
        private Iterator<Tuple> curIterator;
        private int curId;
        private int numIterators;
        private TransactionId tid;
        
        public HeapFileIterator(TransactionId id)
        {
            tid = id;
        }
    
        @Override
        public void open()
                throws DbException, TransactionAbortedException
        {
            numIterators = numPages();
            curId = 0;
            curIterator = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), curId), Permissions.READ_ONLY)).iterator();
        }
        
        public boolean hasNext()
                throws DbException, TransactionAbortedException
        {
            while(curId < numIterators)
            {
                if(curIterator.hasNext())
                    return true;
                if(curId == numIterators - 1)
                    return false;
                Database.getBufferPool().releasePage(tid, new HeapPageId(getId(), curId));
                curIterator = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), ++curId), Permissions.READ_ONLY)).iterator();
            }
            return false;
        }
        
        public Tuple next()
                throws DbException, TransactionAbortedException
        {
            if(!hasNext())
                throw new NoSuchElementException();
            return curIterator.next();
        }
        
        public void remove() throws UnsupportedOperationException
        {
            throw new UnsupportedOperationException();
        }
    
        @Override
        public void rewind()
                throws DbException, TransactionAbortedException
        {
            close();
            open();
        }
    
        @Override
        public void close()
        {
            numIterators = curId = 0;
        }
    }
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

