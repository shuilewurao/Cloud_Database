package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {
	public static KVServer server_FIFO;
	public static KVServer server_LRU;
	public static KVServer server_LFU;
	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			// <servername> <"locahol"> <2181>
			
			server_FIFO = new KVServer(50000, 10, "FIFO");
			server_FIFO.clearStorage();
			new Thread(server_FIFO).start();
			server_LRU = new KVServer(51000, 10, "LRU");
			server_LRU.clearStorage();
			server_FIFO.clearStorage();
			new Thread(server_LRU).start();
			server_LFU = new KVServer(52000, 10, "LFU");
			server_LFU.clearStorage();
			new Thread(server_LFU).start();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class); 
		
		clientSuite.addTestSuite(ECSTest.class); 
		clientSuite.addTestSuite(ECSFunction.class); 
		clientSuite.addTestSuite(HashringTest.class); 
		return clientSuite;
	}
	
}
