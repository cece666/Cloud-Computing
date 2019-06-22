package cache;

/**
 * <h1>Cache entry</h1>
 * <p>
 * This class defines the data type CacheEntry which has a key value pair and
 * also information about which entry is previous and which is next in the order
 * </p>
 * 
 * @author Aleena Yunus
 * @version 1.0
 * @since 26.10.2018
 */

public class CacheEntry {
	String key;
	String value;
	String owner;
	CacheEntry prev;
	CacheEntry next;
}