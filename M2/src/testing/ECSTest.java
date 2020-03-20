package testing;

import app_kvServer.IKVServer;
import client.KVStore;
import ecs.ECS;
import ecs.ECSNode;
import ecs.ECSNodeMessage;
import ecs.IECSNode;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class ECSTest extends TestCase {
    private KVStore kvClient;
    private ECS ecs;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public void setUp() {
        try {

            ecs = new ECS("./ecs.config");
            Thread.sleep(2000);

        } catch (Exception ignored) {
        }
    }

    public void tearDown() {
        try {
            ecs.shutdown();
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_addNode() {
        Exception ex = null;
        try {
            ecs.addNodes(3, "FIFO", 10);
            Thread.sleep(2000);

        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertNull(ex);
    }

    @Test
    public void test_startNode() {
        Exception ex = null;
        try {
            ecs.addNodes(3, "FIFO", 10);
            Thread.sleep(2000);
            ecs.start();
            Thread.sleep(2000);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        Map<String, IECSNode> nodes = ecs.getNodes();
        for (IECSNode node : nodes.values()) {
            assertEquals(ECSNodeMessage.ECSNodeFlag.START, node.getFlag());
        }
        assertNull(ex);
    }

    @Test
    public void test_removeNode() {
        Exception ex = null;
        try {
            ecs.addNodes(2, "FIFO", 10);
            Thread.sleep(2000);
            ecs.start();
            Thread.sleep(2000);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        Map<String, IECSNode> map = ecs.getNodes();

        try {
            Collection<String> keys = map.keySet();
            ecs.removeNodes(keys);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertNull(ex);
    }

    @Test
    public void test_removeNonExistNode() {
        Exception ex = null;

        try {
            Collection<String> keys = Arrays.asList("None-1", "None-2");

            ecs.removeNodes(keys);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertNull(ex);
    }

    @Test
    public void test_stop() {
        Exception ex = null;
        try {
            ecs.addNodes(2, "FIFO", 10);
            Thread.sleep(2000);
            ecs.start();
            Thread.sleep(2000);

        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }

        try {
            ecs.stop();
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertNull(ex);
        Map<String, IECSNode> nodes = ecs.getNodes();
        for (IECSNode node : nodes.values()) {
            assertEquals(ECSNodeMessage.ECSNodeFlag.STOP, node.getFlag());
        }
    }

    @Test
    public void test_shutdown() {
        Exception ex = null;

        try {
            ecs.shutdown();
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assert ex == null;
        HashMap<String, ECSNode> nodes = ecs.getAvailableServers();
        for (IECSNode node : nodes.values()) {
            assertEquals(IKVServer.ServerStateType.SHUT_DOWN, node.getServerStateType());
        }
    }
}