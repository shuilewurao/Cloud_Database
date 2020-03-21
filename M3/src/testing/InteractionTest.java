package testing;

import client.KVStore;
import ecs.ECS;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.IOException;

public class InteractionTest extends TestCase {

    private Logger logger = Logger.getRootLogger();

    private ECS ecs;
    private KVStore kvClient;
    private int port = 50005;


    public void setUp() throws IOException, InterruptedException, KeeperException {
        ecs = new ECS("./ecs.config");

        ecs.addNodes(1, "FIFO", 10);
        Thread.sleep(2000);

        ecs.start();
        Thread.sleep(2000);


    }

    public void tearDown() throws Exception {
        ecs.shutdown();
        Thread.sleep(2000);

    }


    @Test
    public void testPut() throws IOException, InterruptedException {

        logger.debug("[TEST] 1");

        kvClient = new KVStore("localhost", port);
        kvClient.connect();

        Thread.sleep(1000);

        String key = "foo2";
        String value = "bar2";
        KVMessage response = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert response != null;

        assertTrue(response.getStatus().name().equals(StatusType.PUT_SUCCESS.name()) || response.getStatus().name().equals(StatusType.PUT_UPDATE.name()));

        Thread.sleep(1000);
        kvClient.disconnect();
    }

    @Test
    public void testUpdate() throws IOException, InterruptedException {

        logger.debug("[TEST] 2");

        KVStore kvClient = new KVStore("localhost", port);
        kvClient.connect();

        Thread.sleep(1000);

        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, initialValue);
            response = kvClient.put(key, updatedValue);

        } catch (Exception e) {
            ex = e;
        }

        assertTrue((ex == null && response.getStatus().name().equals(StatusType.PUT_UPDATE.name())
                && response.getValue().equals(updatedValue)));

        //kvClient.disconnect();
        Thread.sleep(1000);
        kvClient.disconnect();
    }

    @Test
    public void testDelete() throws IOException, InterruptedException {

        logger.debug("[TEST] 3");

        KVStore kvClient = new KVStore("localhost", port);
        kvClient.connect();

        Thread.sleep(1000);

        String key = "deleteTestValue";
        String value = "toDelete";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            Thread.sleep(1000);
            response = kvClient.put(key, "");

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus().name().equals(StatusType.DELETE_SUCCESS.name()));

        //kvClient.disconnect();
        Thread.sleep(1000);
        kvClient.disconnect();
    }

    @Test
    public void testGet() throws IOException, InterruptedException {

        logger.debug("[TEST] 4");

        KVStore kvClient = new KVStore("localhost", port);
        kvClient.connect();

        Thread.sleep(1000);

        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }
        assertTrue(ex == null && (response.getValue().equals("bar")));
        //kvClient.disconnect();
        Thread.sleep(1000);
        kvClient.disconnect();
    }

    @Test
    public void testGetUnsetValue() throws IOException, InterruptedException {

        logger.debug("[TEST] 5");

        KVStore kvClient = new KVStore("localhost", port);
        kvClient.connect();

        Thread.sleep(1000);

        String key = "an unset value";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }


        assertTrue(ex == null && response.getStatus().name().equals(StatusType.GET_ERROR.name()));
        //kvClient.disconnect();
        Thread.sleep(1000);
        kvClient.disconnect();
    }

    @Test
    public void testPutDisconnected() throws Exception {

        logger.debug("[TEST] 6");

        KVStore kvClient = new KVStore("localhost", port);
        kvClient.connect();

        kvClient.disconnect();
        String key = "foo";
        String value = "bar";
        Exception ex = null;

        try {
            kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(ex);
        kvClient.disconnect();

    }

}
