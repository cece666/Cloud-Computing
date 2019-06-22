package testing;

import app_kvServer.KVServer;
import common.metadata.AddressPort;
import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests {

    static {
	try {
	    new KVServer(5555, new AddressPort("localhost", 4999));
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

    public static Test suite() {
	TestSuite clientSuite = new TestSuite("Saclable Storage ServerTest-Suite");
	clientSuite.addTestSuite(ConnectionTest.class);
	clientSuite.addTestSuite(InteractionTest.class);
	clientSuite.addTestSuite(CacheTest.class);
	clientSuite.addTestSuite(StopShutDownAndNotResponsibleTest.class);
	return clientSuite;
    }

}
