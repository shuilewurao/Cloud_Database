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
			new LogSetup("logs/testing/test.log", Level.ALL);
			// <servername> <"locahol"> <2181>
			System.out.println("Starting new server");
			server_FIFO = new KVServer(50000, 10, "FIFO");
			server_FIFO.clearStorage();
			new Thread(server_FIFO).start();
			System.out.println("Starting new server");
			server_LRU = new KVServer(51000, 10, "LRU");
			server_LRU.clearStorage();
			new Thread(server_LRU).start();
			System.out.println("Starting new server");
			server_LFU = new KVServer(52000, 10, "LFU");
			server_LFU.clearStorage();
			new Thread(server_LFU).start();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		//clientSuite.addTestSuite(ConnectionTest.class);
		//clientSuite.addTestSuite(InteractionTest.class);
		//clientSuite.addTestSuite(AdditionalTest.class);
		System.out.println("Running ECSTest");
		clientSuite.addTestSuite(ECSTest.class);
		System.out.println("Running ECSFunctionTest");
		clientSuite.addTestSuite(ECSFunction.class);
		System.out.println("Running HRTest");
		clientSuite.addTestSuite(HashringTest.class);
		return clientSuite;
	}
	
}
