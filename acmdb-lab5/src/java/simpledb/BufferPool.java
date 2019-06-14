package simpledb;

import sun.security.provider.SHA;

import java.io.*;


import java.lang.reflect.Array;
import java.util.*;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	
	public static final int SHARED = 1;
	public static final int EXCLUSIVE = 2;
	private static final int WAIT_TIME = 50;
	private static final int ABORT_BASE_TIME = 200;
	private static final int ABORT_VAR_TIME = 200;	// abort time in [Base, Base + Var] millisecond
	
	private class PageLock
	{
		public TransactionId tid;
		public PageId pid;
		public int type;
		
		PageLock(TransactionId transactionId, PageId pageId, int t)
		{
			tid = transactionId;
			pid = pageId;
			type = t;
		}
		
		public String toString()
		{
			return "(" + tid + " " + pid.pageNumber() + " " + ((type == EXCLUSIVE) ? "EXCLUSIVE" : "SHARED") + ")";
		}
	}
	
	/** Bytes per page, including header. */
	private static final int PAGE_SIZE = 4096;
	
	private static int pageSize = PAGE_SIZE;
	
	/** Default number of pages passed to the constructor. This is used by
	 other classes. BufferPool should use the numPages argument to the
	 constructor instead. */
	public static final int DEFAULT_PAGES = 50;
	private static int time = 0;
	
	private static int maxPages = DEFAULT_PAGES;
	private static Map<PageId, Page> idToPage;
	private static Map<PageId, Integer> idToTime;
	
	private static Map<TransactionId, ArrayList<PageLock>> tidToLock;
	private static Map<PageId, ArrayList<PageLock>> pidToExclusive;
	private static Map<PageId, ArrayList<PageLock>> pidToShared;
	
	private static Random rand = new Random();
	
	private static final int LRU_POLICY = 1;
	private static final int RANDOM_POLICY = 2;	// may not support multi-threads
	private static final int EVICT_POLICY = LRU_POLICY;
	
	/**
	 * Prints all the locks of a page. Just for debugging.
	 * @param pp
	 */
	public synchronized void printPidLock(PageId pp)
	{
		System.err.println("PID: " + pp.pageNumber() + "\nExclusive:");
		ArrayList<PageLock> pll = pidToExclusive.get(pp);
		for(PageLock plll : pll)
		{
			System.err.println(plll.toString());
		}
		System.err.println("Shared:");
		pll = pidToShared.get(pp);
		for(PageLock plll : pll)
		{
			System.err.println(plll.toString());
		}
	}
	
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		maxPages = numPages;
		idToPage = new HashMap<>();
		idToTime = new HashMap<>();
		tidToLock = new HashMap<>();
		pidToExclusive = new HashMap<>();
		pidToShared = new HashMap<>();
	}
	
	public static int getPageSize() {
		return pageSize;
	}
	
	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}
	
	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = PAGE_SIZE;
	}
	
	/**
	 * Retrieve the specified page with the associated permissions.
	 * Will acquire a lock and may block if that lock is held by another
	 * transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool.  If it
	 * is present, it should be returned.  If it is not present, it should
	 * be added to the buffer pool and returned.  If there is insufficient
	 * space in the buffer pool, an page should be evicted and the new page
	 * should be added in its place.
	 *
	 * @param tid the ID of the transaction requesting the page
	 * @param pid the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException
	{
		// some code goes here
		PageLock pageLock;
		if(!pidToExclusive.containsKey(pid))
			pidToExclusive.put(pid, new ArrayList<>());
		if(!pidToShared.containsKey(pid))
			pidToShared.put(pid, new ArrayList<>());
		if(!tidToLock.containsKey(tid))
			tidToLock.put(tid, new ArrayList<>());
		long stTime = System.currentTimeMillis();
		if(perm == Permissions.READ_ONLY)
		{
			while(!pidToExclusive.get(pid).isEmpty())
			{
				long edTime = System.currentTimeMillis();
				if(pidToExclusive.get(pid).get(0).tid == tid)
					break;
				if(edTime - stTime > ABORT_BASE_TIME + rand.nextInt(ABORT_VAR_TIME))
				{
					//System.err.println(tid + " aborted");
					throw new TransactionAbortedException();
				}
				try
				{
					wait(WAIT_TIME);
				} catch(InterruptedException e) {}
			}
			pageLock = new PageLock(tid, pid, SHARED);
			if(!pidToShared.containsKey(pid))
				pidToShared.put(pid, new ArrayList<>());
			pidToShared.get(pid).add(pageLock);
		}
		else
		{
			while(!pidToExclusive.get(pid).isEmpty() || !pidToShared.get(pid).isEmpty())
			{
				long edTime = System.currentTimeMillis();
				if(pidToExclusive.get(pid).isEmpty())
				{
					Iterator<PageLock> it = pidToShared.get(pid).iterator();
					boolean single = true;
					while(it.hasNext())
					{
						PageLock pl = it.next();
						if(pl.tid != tid)
						{
							single = false;
							break;
						}
					}
					if(single)
						break;
				}
				else if(pidToExclusive.get(pid).get(0).tid == tid)
					break;
				if(edTime - stTime > ABORT_BASE_TIME + rand.nextInt(ABORT_VAR_TIME))
				{
					//System.err.println(tid + " aborted");
					throw new TransactionAbortedException();
				}
				try
				{
					wait(WAIT_TIME);
				} catch(InterruptedException e) {}
			}
			pageLock = new PageLock(tid, pid, EXCLUSIVE);
			if(!pidToExclusive.containsKey(pid))
				pidToExclusive.put(pid, new ArrayList<>());
			pidToExclusive.get(pid).add(pageLock);
		}
		tidToLock.get(tid).add(pageLock);
		
		if(idToPage.containsKey(pid))
		{
			idToTime.put(pid, time++);
			return idToPage.get(pid);
		}
		else
		{
			if(idToPage.size() >= maxPages)
			{
				evictPage();
			}
			Page rtn = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
			idToPage.put(pid, rtn);
			idToTime.put(pid, time++);
			return rtn;
		}
	}
	
	/**
	 * Provide a method to ensure that the page is in the BufferPool before marked dirty.
	 * Only used by insertTuple() and deleteTuple()
	 * If the page is already in the BufferPool (but as an old version), repalce it.
	 * Otherwise, instead of getting it through the Catalog, use the page provided.
	 */
	private synchronized void replacePage(TransactionId tid, Page page, Permissions perm)
		throws TransactionAbortedException, DbException
	{
		PageId pid = page.getId();
		if(idToPage.containsKey(pid))
		{
			idToTime.put(pid, time++);
			idToPage.replace(pid, page);
		}
		else
		{
			if(idToPage.size() >= maxPages)
			{
				evictPage();
			}
			idToPage.put(pid, page);
			idToTime.put(pid, time++);
		}
	}
	
	/**
	 * Releases the lock on a page.
	 * Calling this is very risky, and may result in wrong behavior. Think hard
	 * about who needs to call this and why, and why they can run the risk of
	 * calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public synchronized void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		if(tidToLock.containsKey(tid))
		{
			Iterator<PageLock> it = tidToLock.get(tid).iterator();
			while(it.hasNext())
			{
				PageLock pl = it.next();
				if(pl.pid == pid)
					it.remove();
			}
		}
		if(pidToExclusive.containsKey(pid))
		{
			Iterator<PageLock> it = pidToExclusive.get(pid).iterator();
			while(it.hasNext())
			{
				PageLock pl = it.next();
				if(pl.tid == tid)
					it.remove();
			}
		}
		if(pidToShared.containsKey(pid))
		{
			Iterator<PageLock> it = pidToShared.get(pid).iterator();
			while(it.hasNext())
			{
				PageLock pl = it.next();
				if(pl.tid == tid)
					it.remove();
			}
		}
		notifyAll();
	}
	
	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}
	
	/** Return true if the specified transaction has a lock on the specified page */
	public synchronized boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		Iterator<PageLock> it = tidToLock.get(tid).iterator();
		while(it.hasNext())
		{
			PageLock pl = it.next();
			if(pl.pid == p)
			{
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Commit or abort a given transaction; release all locks associated to
	 * the transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public synchronized void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		if(commit)
		{
			flushPages(tid);
		}
		else
		{
			/*
			 * If a transaction is aborted, some pages may have been modified yet not marked dirty.
			 * So discard all the pages with exclusive lock.
			 */
			if(tidToLock.containsKey(tid))
			{
				Iterator<PageLock> it = tidToLock.get(tid).iterator();
				while(it.hasNext())
				{
					PageLock pl = it.next();
					if(pl.type == EXCLUSIVE)
					{
						discardPage(pl.pid);
					}
				}
				
			}
		}
		if(!tidToLock.containsKey(tid))
		{
			notifyAll();
			return;
		}
		ArrayList<PageLock> pageLocks = tidToLock.get(tid);
		for(PageLock pl : pageLocks)
		{
			PageId pageId = pl.pid;
			Iterator<PageLock> it = pidToExclusive.get(pageId).iterator();
			while(it.hasNext())
			{
				PageLock pageLock = it.next();
				if(pageLock.tid == tid)
				{
					it.remove();
				}
			}
			it = pidToShared.get(pageId).iterator();
			while(it.hasNext())
			{
				PageLock pageLock = it.next();
				if(pageLock.tid == tid)
				{
					it.remove();
				}
			}
		}
		pageLocks.clear();
		tidToLock.remove(tid);
		notifyAll();
	}
	
	/**
	 * Add a tuple to the specified table on behalf of transaction tid.  Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed for lab2).
	 * May block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have
	 * been dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t the tuple to add
	 */
	public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		DbFile file = Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> dirty = file.insertTuple(tid, t);
		
		synchronized(this)
		{
			for(Page p : dirty)
			{
				getPage(tid, p.getId(), Permissions.READ_WRITE);
				replacePage(tid, p, Permissions.READ_WRITE);
				p.markDirty(true, tid);
			}
		}
	}
	
	/**
	 * Remove the specified tuple from the buffer pool.
	 * Will acquire a write lock on the page the tuple is removed from and any
	 * other pages that are updated. May block if the lock(s) cannot be acquired.
	 *
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have
	 * been dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t the tuple to delete
	 */
	public synchronized void deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> dirty = file.deleteTuple(tid, t);
		synchronized(this)
		{
			for(Page p : dirty)
			{
				getPage(tid, p.getId(), Permissions.READ_WRITE);
				replacePage(tid, p, Permissions.READ_WRITE);
				p.markDirty(true, tid);
			}
		}
	}
	
	/**
	 * Flush all dirty pages to disk.
	 * NB: Be careful using this routine -- it writes dirty data to disk so will
	 *     break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for lab1
		for(Map.Entry<PageId, Page> entry : idToPage.entrySet())
		{
			flushPage(entry.getKey());
		}
	}
	
	/** Remove the specific page id from the buffer pool.
	 Needed by the recovery manager to ensure that the
	 buffer pool doesn't keep a rolled back page in its
	 cache.
	 
	 Also used by B+ tree files to ensure that deleted pages
	 are removed from the cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1
		if(idToTime.containsKey(pid))
		{
			idToTime.remove(pid);
			idToPage.remove(pid);
			idToPage.remove(pid);
		}
	}
	
	/**
	 * Flushes a certain page to disk
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized  void flushPage(PageId pid) {
		// some code goes here
		// not necessary for lab1
		try
		{
			if(idToPage.get(pid).isDirty() != null)
			{
				Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(idToPage.get(pid));
				idToPage.get(pid).markDirty(false, null);
			}
		}
		catch(IOException e)
		{
			System.err.println("Flush Page Failed.");
		}
	}
	
	/** Write all pages of the specified transaction to disk.
	 */
	public synchronized  void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		if(!tidToLock.containsKey(tid))
			return;
		ArrayList<PageLock> pageLocks = tidToLock.get(tid);
		for(PageLock pl : pageLocks)
		{
			if(pl.type == EXCLUSIVE && idToPage.containsKey(pl.pid))
			{
				flushPage(pl.pid);
			}
		}
	}
	
	/**
	 * Decides which page to discard.
	 *
	 */
	private synchronized PageId choosePage() throws DbException
	{
		switch(EVICT_POLICY)
		{
			case LRU_POLICY:
				PageId rtn = (PageId)idToTime.keySet().toArray()[0];
				int minTime = Integer.MAX_VALUE;
				boolean success = false;
				for(Map.Entry<PageId, Integer> entry : idToTime.entrySet())
				{
					if(entry.getValue() < minTime && idToPage.get(entry.getKey()).isDirty() == null)
					{
						success = true;
						rtn = entry.getKey();
						minTime = entry.getValue();
					}
				}
				if(!success)
				{
					throw new DbException("No page is clean, cannot choose one to evict.");
				}
				return rtn;
			case RANDOM_POLICY:
				Object[] keys = idToPage.keySet().toArray();
				int rd = (int)(Math.random() * idToPage.size());
				return (PageId)keys[rd];
		}
		return null;
	}
	
	/**
	 * Discards a page from the buffer pool.
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 */
	private synchronized  void evictPage() throws DbException {
		// some code goes here
		// not necessary for lab1
		PageId discardPageId = choosePage();
		flushPage(discardPageId);
		discardPage(discardPageId);
	}
	
}
