package testing;

import ecs.ECSHashRing;
import ecs.ECSNode;
import junit.framework.TestCase;

import java.util.concurrent.CountDownLatch;


public class HashRingTest extends TestCase {
    private ECSHashRing hr_test;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private int numGetClients = 5;

    public void setUp() {
        try {
            hr_test = new ECSHashRing();
        } catch (Exception e) {
            //System.out.println("ECS Test error "+e);
        }
    }

    public void tearDown() {
        hr_test.removeAllNode();
        hr_test = null;
    }

    public void test_addNodes() {
        Exception ex = null;
        try {
            hr_test.addNode(new ECSNode("server1", "localhost", 50001));
            hr_test.addNode(new ECSNode("server2", "localhost", 50002));
            hr_test.addNode(new ECSNode("server3", "localhost", 50003));
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    public void test_getNodes() {
        Exception ex = null;
        try {
            ECSNode temp = new ECSNode("server4", "localhost", 50004);
            hr_test.getNodeByServerName("server4");
            hr_test.getNodeByHash(temp.getNodeHashBI());
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);
    }

    public void test_removeNodes() {

        ECSNode temp = new ECSNode("server4", "localhost", 50004);

        hr_test.addNode(new ECSNode("server3", "localhost", 50003));

        Exception ex = null;
        try {
            hr_test.removeNode(temp);
        } catch (Exception e) {
            ex = e;
            e.printStackTrace();
        }
        assertNull(ex);
    }
}