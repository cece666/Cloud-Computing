package cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <h1>FIFO Cache</h1>
 * <p>
 * This class acts as the child class the ServerCache class.It over-rides the
 * abstract methods to make the implementation specific to First In First Out.
 * i.e. the entry that was added first is evicted upon the cache becoming full.
 * </p>
 * 
 * @author Aleena Yunus
 * @version 1.0
 * @since 26.10.2018
 */
public class FIFOCache extends ServerCache {

	/**
	 * logger object for this class
	 */
	private static Logger logger = LogManager.getLogger(FIFOCache.class);

	/**
	 * Constructor for specifying size.
	 * 
	 * @param CacheSize CacheSize for the cache specified at the start of the server
	 */
	public FIFOCache(int CacheSize) {
		super(CacheSize);
	}

	/**
	 * This method takes a String as a key and upon checking whether the cache
	 * contains that key returns the value for that key.
	 * 
	 * @param key The key to be searched for and whose corresponding value is
	 *            returned.
	 * @return entry.value which is the value of the given key.
	 */
	@Override
	public synchronized String get(String key, String owner) {
		if (serverCache.containsKey(key)) {
			CacheEntry entry = serverCache.get(key);
			if (entry.owner.equals(owner)) {
				logger.debug(key + "-" + serverCache.get(key).value + " accessed from cache");
				return serverCache.get(key).value;
			}
		}
		logger.debug("Cache miss for " + key);
		return null;
	}

	/**
	 * This method takes two Strings as key and value and adds them to the top of
	 * the cache. In case of the cache becoming full, the tail of the cache which
	 * was the first entry added is removed.
	 * 
	 * @param key   The key to be added
	 * @param value The value to be added
	 * @return Nothing.
	 */
	@Override
	public synchronized void put(String key, String value, String owner) {
		CacheEntry entry = new CacheEntry();
		entry.key = key;
		entry.value = value;
		entry.owner = owner;
		if (serverCache.size() == size) {
			serverCache.remove(tail.key);
			remove(tail);
		}
		putOnTop(entry);
		serverCache.put(key, entry);
		logger.debug(key + "-" + entry.value + " put in cache");
	}

	/**
	 * This method removes an entry from the cache, given its key
	 * 
	 * @param key The key whose entry is to be deleted.
	 * @return Nothing.
	 */
	@Override
	public synchronized void invalidate(String key) {
		CacheEntry entry = serverCache.remove(key);
		remove(entry);
	}

}