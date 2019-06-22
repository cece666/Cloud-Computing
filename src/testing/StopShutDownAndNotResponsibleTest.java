package testing;

import java.io.IOException;

import org.junit.Test;

import app_kvEcs.ECSClient;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.StatusType;
import junit.framework.TestCase;

/**
 * <h1>Cache Test</h1>
 * <p>
 * This class, extending TestCase, defines five types of tests which test the
 * server stopped, shutdown and not responsible scenarios.
 * </p>
 * 
 * @author Aleena Yunus
 * @version 1.0
 * @since 26.10.2018
 */
public class StopShutDownAndNotResponsibleTest extends TestCase {

    private KVStore kvClient;
    private ECSClient ecsClient;

    public void setUp() throws IOException {
	ecsClient = new ECSClient();
	kvClient = new KVStore("localhost", 5001);
	try {
	    ecsClient.initService(5, 4, "FIFO");
	    ecsClient.start();
	    kvClient.connect();
	} catch (Exception e) {
	}
    }

    /**
     * tests the server shutdown functionality
     * 
     * @param None.
     * @return Nothing.
     */
    @Test
    public void testServerShutdown() {
	KVMessage response = null;
	Exception ex = null;

	try {
	    ecsClient.shutDown();
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null);
    }

    /**
     * tests writing data to server when it is stopped
     * 
     * @param None.
     * @return Nothing.
     */
    @Test
    public void testServerStopped() {

	String key = "foo";
	String value = "bar";
	KVMessage response = null;
	Exception ex = null;

	try {
	    ecsClient.stop();
	    response = kvClient.put(key, value);
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && response.getStatus() == StatusType.SERVER_STOPPED);
    }

    /**
     * tests writing data to server when key is out of the range of server
     * 
     * @param None.
     * @return Nothing.
     */
    @Test
    public void testPutWithOutofRangeKey() {

	String key = "12345678910";
	String value = "bar";
	KVMessage response = null;
	Exception ex = null;

	try {
	    response = kvClient.put(key, value);
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);
    }

    /**
     * tests getting data from server when key is out of the range of server
     * 
     * @param None.
     * @return Nothing.
     */
    @Test
    public void testGetWithOutofRangeKey() {

	String key = "12345678910";
	Exception ex = null;
	KVMessage response = null;

	try {
	    response = kvClient.get(key);
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);
    }

    /**
     * tests deleting data from server when key is out of the range of server
     * 
     * @param None.
     * @return Nothing.
     */
    @Test
    public void testDeleteWithOutofRangeKey() {

	String key = "12345678910";
	String value = null;
	KVMessage response = null;
	Exception ex = null;

	try {
	    response = kvClient.put(key, value);
	} catch (Exception e) {
	    ex = e;
	}

	assertTrue(ex == null && response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE);
    }

}