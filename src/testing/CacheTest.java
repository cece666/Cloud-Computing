package testing;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import cache.FIFOCache;
import cache.LFUCache;
import cache.LRUCache;
import cache.ServerCache;
import junit.framework.TestCase;

/**
 * <h1>Cache Test</h1>
 * <p>
 * This class, extending TestCase, defines three types of tests which test the
 * three cache eviction strategies.
 * </p>
 * 
 * @author Aleena Yunus
 * @version 1.0
 * @since 26.10.2018
 */
public class CacheTest extends TestCase {

    /**
     * Test cache size
     */
    private static final int SIZE = 3;

    /**
     * This method creates a FIFOCache object, adds and accesses entries to and from
     * the cache and then adds another entry which will evict one of the earlier
     * entries. Then it compares the resulting cache to the expected cache values,
     * and fails the test if both do not match.
     * 
     * @param None.
     * @return Nothing.
     */
    @Test
    public void testFIFOCacheRemoval() {

	ServerCache serverCache = new FIFOCache(SIZE);

	serverCache.put("1", "100");
	serverCache.put("2", "200");
	serverCache.put("3", "300");
	serverCache.get("3");
	serverCache.get("3");
	serverCache.get("2");
	serverCache.get("1");
	serverCache.get("1");
	serverCache.put("4", "400");

	Set<String> correctKeys = new HashSet<>();
	correctKeys.add("2");
	correctKeys.add("3");
	correctKeys.add("4");

	assertTrue(serverCache.getKeys().equals(correctKeys));
    }

    /**
     * This method creates a LRUCache object, adds and accesses entries to and from
     * the cache and then adds another entry which will evict one of the earlier
     * entries. Then it compares the resulting cache to the expected cache values,
     * and fails the test if both do not match.
     * 
     * @param None.
     * @return Nothing.
     */
    @Test
    public void testLRUCacheRemoval() {
	ServerCache serverCache = new LRUCache(SIZE);

	serverCache.put("1", "100");
	serverCache.put("2", "200");
	serverCache.put("3", "300");
	serverCache.get("3");
	serverCache.get("3");
	serverCache.get("2");
	serverCache.get("1");
	serverCache.get("1");
	serverCache.put("4", "400");

	Set<String> correctKeys = new HashSet<>();
	correctKeys.add("1");
	correctKeys.add("2");
	correctKeys.add("4");

	assertTrue(serverCache.getKeys().equals(correctKeys));
    }

    /**
     * This method creates a LFUCache object, adds and accesses entries to and from
     * the cache and then adds another entry which will evict one of the earlier
     * entries. Then it compares the resulting cache to the expected cache values,
     * and fails the test if both do not match.
     * 
     * @param None.
     * @return Nothing.
     */
    @Test
    public void testLFUCacheRemoval() {
	ServerCache serverCache = new LFUCache(SIZE);

	serverCache.put("1", "100");
	serverCache.put("2", "200");
	serverCache.put("3", "300");
	serverCache.get("3");
	serverCache.get("3");
	serverCache.get("2");
	serverCache.get("1");
	serverCache.get("1");
	serverCache.put("4", "400");

	Set<String> correctKeys = new HashSet<>();
	correctKeys.add("1");
	correctKeys.add("3");
	correctKeys.add("4");

	assertTrue(serverCache.getKeys().equals(correctKeys));
    }
}
