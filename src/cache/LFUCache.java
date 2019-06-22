package cache;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <h1>LRU Cache</h1>
 * <p>
 * This class acts as the child class the ServerCache class.It over-rides the
 * abstract methods to make the implementation specific to Least Frequently
 * Used. i.e. the entry that was accessed the least recently is evicted upon the
 * cache becoming full.
 * </p>
 * 
 * @author Aleena Yunus
 * @version 1.0
 * @since 26.10.2018
 */
public class LFUCache extends ServerCache {

	/**
	 * timeCounter and TS to fix the see which entry was least recently used in case
	 * of tie. Freq hashmap to see which entry was most frequently accessed.
	 */
	private HashMap<String, Integer> Freq;
	private HashMap<String, Double> TS;
	private double timeCounter = 0;

	/**
	 * logger object for this class
	 */
	private static Logger logger = LogManager.getLogger(LFUCache.class);

	/**
	 * Constructor for specifying size and initializing cache and TS.
	 * 
	 * @param CacheSize CacheSize for the cache specified at the start of the server
	 */
	public LFUCache(int CacheSize) {
		super(CacheSize);
		Freq = new HashMap<>(CacheSize);
		TS = new HashMap<>(CacheSize);
	}

	/**
	 * This method takes a String as a key and upon checking whether the cache
	 * contains that key returns the value for that key and also increments the
	 * frequency and the time stamp for that entry.
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
				int value = Freq.get(key);
				Freq.replace(key, value + 1);
				timeCounter = timeCounter + 0.1;
				TS.replace(key, timeCounter);
				logger.debug(key + "-" + entry.value + " accessed from cache");
				return entry.value;
			}
		}
		logger.debug("Cache miss for " + key);
		return null;
	}

	/**
	 * This method takes two Strings as key and value and adds them to the top of
	 * the cache. In case of the cache becoming full, the entry with the least
	 * frequency is removed. In case of two entries having the same least frequency,
	 * then the one with the smallest access time stamp is used.
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
			ArrayList<String> minKeys = minKeys();
			if (minKeys.size() == 1) {
				RemoveLFU(minKeys.get(0));
			} else {
				RemoveLRUinLFU(minKeys);
			}
		}
		putOnTop(entry);
		serverCache.put(key, entry);
		Freq.put(key, 0);
		timeCounter = timeCounter + 0.1;
		TS.put(key, timeCounter);
		logger.debug(key + "-" + entry.value + " put in cache");

	}

	/**
	 * This method gives all the keys with the minimum frequency.
	 * 
	 * @param None.
	 * @return minKeys which is the list containing all min access frequency keys.
	 */
	private ArrayList<String> minKeys() {
		ArrayList<String> minKeys = new ArrayList<String>();
		int minFreq = Integer.MAX_VALUE;

		for (HashMap.Entry<String, Integer> entry : Freq.entrySet()) {
			if (minFreq > entry.getValue()) {
				minFreq = entry.getValue();
			}
		}

		for (HashMap.Entry<String, Integer> entry : Freq.entrySet()) {
			if (minFreq == entry.getValue()) {
				minKeys.add(entry.getKey());
			}
		}

		return minKeys;
	}

	/**
	 * This method removes the lest frequently accessed cache entry, given its key
	 * 
	 * @param minKey The key whose entry is to be deleted.
	 * @return Nothing.
	 */
	private synchronized void RemoveLFU(String minKey) {
		CacheEntry temp = serverCache.get(minKey);
		serverCache.remove(temp.key);
		remove(temp);
		Freq.remove(minKey);
		TS.remove(minKey);
	}

	/**
	 * This method removes the lest recently accessed cache entry, given a set of
	 * minimum access frequency keys.
	 * 
	 * @param minKeys The keys with the minimum access frequency.
	 * @return Nothing.
	 */
	private synchronized void RemoveLRUinLFU(ArrayList<String> minKeys) {
		String remKey = minKeys.get(0);
		double minTime = TS.get(minKeys.get(0));
		for (int i = 0; i < minKeys.size(); i++) {
			if (minTime > TS.get(minKeys.get(i))) {
				remKey = minKeys.get(i);
				minTime = TS.get(minKeys.get(i));
			}
		}

		CacheEntry temp = serverCache.get(remKey);
		serverCache.remove(temp.key);
		remove(temp);
		Freq.remove(remKey);
		TS.remove(remKey);

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
		Freq.remove(key);
		TS.remove(key);
		remove(entry);

	}

}