package cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <h1>Server Cache</h1>
 * <p>
 * This class acts as the parent class for the three cache implementation
 * strategies It uses the data structure of a linked hashmap in the form of a
 * doubly linked list which is modified differently according to the different
 * strategies by overriding the abstract methods.
 * </p>
 * 
 * @author Aleena Yunus
 * @version 1.0
 * @since 26.10.2018
 */

public abstract class ServerCache {
	/**
	 * size integer to fix the size of cache, Map variable to store key value pairs
	 * and head and tail CacheEntry variables to maintain the doubly linked list
	 */
	protected final int size;
	protected Map<String, CacheEntry> serverCache;
	protected CacheEntry head, tail;

	public static final String FIFO = "FIFO";
	public static final String LRU = "LRU";
	public static final String LFU = "LFU";
	/**
	 * logger object for this class
	 */
	private static Logger logger = LogManager.getLogger(ServerCache.class);

	/**
	 * Constructor for specifying size and initializing LinkedHashMap
	 * 
	 * @param size Size for the cache specified at the start of the server
	 */
	public ServerCache(int size) {
		this.size = size;
		serverCache = new LinkedHashMap<>(size);
	}

	/**
	 * This method returns the set of keys contained in the cache
	 * 
	 * @param None
	 * @return serverCache.keySet() which gives the set of keys contained in the
	 *         hashmap.
	 */
	public Set<String> getKeys() {
		return serverCache.keySet();
	}

	/**
	 * This method prints all the key value pairs that the cache contains.
	 * 
	 * @param None
	 * @return Nothing
	 */
	public void printContents() {
		Set<String> keys = serverCache.keySet();
		for (String k : keys) {
			logger.info(k + "-" + serverCache.get(k).value);
		}
	}

	/**
	 * Abstract method over-ridden by child classes in order to get the value
	 * corresponding to a specific key.
	 */
	public abstract String get(String key, String owner);

	/**
	 * Abstract method over-ridden by child classes in order to insert a key value
	 * pair.
	 */
	public abstract void put(String key, String value, String owner);

	/**
	 * Abstract method over-ridden by child classes in order to remove an entry in
	 * the cache.
	 */
	public abstract void invalidate(String key);

	/**
	 * This method takes a cache entry and removes it from the linked list by
	 * changing the prev and next pointers of it's corresponding prev and next
	 * entries or changing the head and tail values if it is at either of the edges.
	 * 
	 * @param entry it is the CacheEntry to be removed.
	 * @return Nothing
	 */
	protected synchronized void remove(CacheEntry entry) {
		if (entry == null)
			return;
		if (entry.prev != null) {
			entry.prev.next = entry.next;
		} else {
			head = entry.next;
		}
		if (entry.next != null) {
			entry.next.prev = entry.prev;
		} else {
			tail = entry.prev;
		}

		logger.debug(entry.key + "-" + entry.value + " evicted from cache");

	}

	/**
	 * This method puts an entry at the front of the list by making it's next
	 * pointer point to the previous head making the head's prev equal to the entry,
	 * in case of the list not being empty and then making the head as the entry. If
	 * the entry is the first element added we also make the tail equal to the
	 * entry.
	 * 
	 * @param entry it is the CacheEntry to be put on top
	 * @return Nothing
	 */
	protected synchronized void putOnTop(CacheEntry entry) {
		entry.next = head;
		if (head != null)
			head.prev = entry;
		head = entry;
		if (tail == null)
			tail = head;
	}
}