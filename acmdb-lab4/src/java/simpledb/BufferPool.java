package simpledb;

import java.io.*;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


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
	
	private static final int LRU_POLICY = 1;
	private static final int RANDOM_POLICY = 2;
	private static final int EVICT_POLICY = LRU_POLICY;
	
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
	public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException
	{
		// some code goes here
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
	private void replacePage(TransactionId tid, Page page, Permissions perm)
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
	public  void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
	}
	
	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
	}
	
	/** Return true if the specified transaction has a lock on the specified page */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		return false;
	}
	
	/**
	 * Commit or abort a given transaction; release all locks associated to
	 * the transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
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
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		DbFile file = Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> dirty = file.insertTuple(tid, t);
		for(Page p : dirty)
		{
			replacePage(tid, p, Permissions.READ_WRITE);
			p.markDirty(true, tid);
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
	public  void deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> dirty = file.deleteTuple(tid, t);
		for(Page p : dirty)
		{
			replacePage(tid, p, Permissions.READ_WRITE);
			p.markDirty(true, tid);
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
		idToTime.remove(pid);
		idToPage.remove(pid);
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
	}
	
	/**
	 * Decides which page to discard.
	 *
	 */
	private synchronized PageId choosePage()
	{
		switch(EVICT_POLICY)
		{
			case LRU_POLICY:
				PageId rtn = (PageId)idToTime.keySet().toArray()[0];
				int minTime = Integer.MAX_VALUE;
				for(Map.Entry<PageId, Integer> entry : idToTime.entrySet())
				{
					if(entry.getValue() < minTime)
					{
						rtn = entry.getKey();
						minTime = entry.getValue();
					}
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
