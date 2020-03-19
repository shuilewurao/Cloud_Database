package testing;

import java.io.IOException;

import ecs.ECS;
import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.zookeeper.KeeperException;


public class AllTests {
	public static KVServer server_FIFO;
	public static KVServer server_LRU;
	public static KVServer server_LFU;
	public static ECS ecs;

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ALL);

			ecs = new ECS("./ecs.config");
			ecs.addNodes(1, "FIFO", 10);
			Thread.sleep(2000);

			ecs.start();
			Thread.sleep(2000);

			server_FIFO = new KVServer(51000, 10, "FIFO");
			server_FIFO.clearStorage();
			server_FIFO.start();
			new Thread(server_FIFO).start();

			server_LRU = new KVServer(52000, 10, "LRU");
			server_LRU.clearStorage();
			server_LRU.start();
			new Thread(server_LRU).start();

			server_LFU = new KVServer(53000, 10, "LFU");
			server_LFU.clearStorage();
			server_LFU.start();
			new Thread(server_LFU).start();

		} catch (IOException | InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	public static Test suite() throws Exception {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");

		clientSuite.addTestSuite(ConnectionTest.class);

		clientSuite.addTestSuite(HashRingTest.class);
		Thread.sleep(1000);

		clientSuite.addTestSuite(InteractionTest.class);
		Thread.sleep(1000);

		clientSuite.addTestSuite(ECSTest.class);
		Thread.sleep(1000);

		//clientSuite.addTestSuite(AdditionalTest.class);

		return clientSuite;
	}
}
