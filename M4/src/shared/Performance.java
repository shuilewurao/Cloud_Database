package shared;

import client.KVStore;
import ecs.ECS;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import shared.messages.KVMessage;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

public class Performance {

    private Logger logger = Logger.getRootLogger();

    private long totalGetLatency;
    private long totalPutLatency;
    private long add1NodesLatency;
    private long add5NodesLatency;
    private long add10NodesLatency;
    private long remove1NodesLatency;
    private long remove5NodesLatency;
    private long remove10NodesLatency;

    private ECS ecs;
    private String CACHE_STRATEGY;
    private Integer CACHE_SIZE;
    private KVStore client;
    private static List<String> msgs = DataParser.parseDataFrom("allen-p/inbox");

    public Performance(int cacheSize, String strategy) throws Exception {
        this.totalGetLatency = 0;
        this.totalPutLatency = 0;

        this.CACHE_SIZE = cacheSize;
        this.CACHE_STRATEGY = strategy;

        //initialize and start the required number of servers
        ecs = new ECS("./ecs.config");
        Thread.sleep(1000);

        this.addNodes(1);
        Thread.sleep(1000);

        this.client = new KVStore("localhost", 50000);
        this.client.connect();

        Thread.sleep(1000);
    }

    public void addNodes(Integer count) {

        try {
            ecs.addNodes(count, this.CACHE_STRATEGY, this.CACHE_SIZE);
            Thread.sleep(10000);
            ecs.start();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void removeNodes() {
        Exception ex = null;

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

    public void startTest() throws Exception {

        /* ----- PUT ----- */
        long start = System.nanoTime();
        for (String msg : msgs) {

            String[] tokens = msg.split("\\" + Constants.DELIMITER);

            KVMessage ret = this.client.put(tokens[0], tokens[1]);
        }

        long end = System.nanoTime();
        this.totalPutLatency = end - start;

        /* ----- GET ----- */
        start = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            int randIndex = new Random().nextInt(msgs.size() - 1);
            String msg1 = msgs.get(randIndex);
            String[] tokens1 = msg1.split("\\" + Constants.DELIMITER);
            KVMessage ret = this.client.get(tokens1[0]);
            assertEquals(KVMessage.StatusType.GET_SUCCESS, ret.getStatus());
            assertEquals(tokens1[1], ret.getValue());
        }
        end = System.nanoTime();
        this.totalGetLatency = end - start;

        this.client.disconnect();
        this.removeNodes();
        Thread.sleep(3000);

        /* ----- ADD1 ----- */
        System.out.println("Add 1");
        start = System.nanoTime();
        this.addNodes(1);
        end = System.nanoTime();
        this.add1NodesLatency = end - start;
        Thread.sleep(3000);

        /* ----- REMOVE1 ----- */
        System.out.println("Remove 1");
        start = System.nanoTime();
        this.removeNodes();
        end = System.nanoTime();
        this.remove1NodesLatency = end - start;
        Thread.sleep(3000);

        /* ----- ADD5 ----- */
        System.out.println("Add 5");
        start = System.nanoTime();
        this.addNodes(5);
        end = System.nanoTime();
        this.add5NodesLatency = end - start;
        Thread.sleep(5000);

        /* ----- REMOVE5 ----- */
        System.out.println("Remove 5");
        start = System.nanoTime();
        this.removeNodes();
        end = System.nanoTime();
        this.remove5NodesLatency = end - start;
        Thread.sleep(5000);

        /* ----- ADD10 ----- */
        System.out.println("Add 10");
        start = System.nanoTime();
        this.addNodes(10);
        end = System.nanoTime();
        this.add10NodesLatency = end - start;
        Thread.sleep(10000);

        /* ----- REMOVE10 ----- */
        System.out.println("Remove 10");
        start = System.nanoTime();
        this.removeNodes();
        end = System.nanoTime();
        this.remove10NodesLatency = end - start;

    }

    public void averageLatency() {
        float totalAvgLat = (this.totalGetLatency + this.totalPutLatency) / (float) msgs.size();
        System.out.println("\tTotal Average Latency: " + totalAvgLat / 1000000);
    }

    public void averagePutLatency() {
        float averagePutLat = this.totalPutLatency / (float) msgs.size();
        System.out.println("\tAverage PUT Latency: " + averagePutLat / 1000000);
    }

    public void averageGetLatency() {
        float averageGetLat = this.totalGetLatency / (float) msgs.size();
        System.out.println("\tAverage GET Latency: " + averageGetLat / 1000000);
    }

    public void get1AddNodesTime() {
        System.out.println("\tAdd 1 Node Latency: " + (this.add1NodesLatency / 1000000 - 10000));
    }

    public void get5AddNodesTime() {
        System.out.println("\tAdd 5 Node Latency: " + (this.add5NodesLatency / 1000000 - 10000));
    }

    public void get10AddNodesTime() {
        System.out.println("\tAdd 10 Node Latency: " + (this.add10NodesLatency / 1000000 - 10000));
    }

    public void get1RemoveNodesTime() {
        System.out.println("\tRemove 1 Node Latency: " + this.remove1NodesLatency / 1000000);
    }

    public void get5RemoveNodesTime() {
        System.out.println("\tRemove 5 Node Latency: " + this.remove5NodesLatency / 1000000);
    }

    public void get10RemoveNodesTime() {
        System.out.println("\tRemove 10 Node Latency: " + this.remove10NodesLatency / 1000000);
    }

    public void Shutdown() throws Exception {
        boolean ret = this.ecs.shutdown();
        Thread.sleep(2000);
        assertTrue(ret);
    }

    public static void main(String[] args) {

        int cacheSize = 30;
        String cacheStrategy = "FIFO";

        try {
            Performance performance = new Performance(cacheSize, cacheStrategy);
            performance.startTest();

            System.out.println("\n********** Latency in milliseconds **********");

            performance.get1AddNodesTime();
            performance.get5AddNodesTime();
            performance.get10AddNodesTime();
            System.out.println(" ");
            performance.get1RemoveNodesTime();
            performance.get5RemoveNodesTime();
            performance.get10RemoveNodesTime();
            System.out.println(" ");
            System.out.println("\t # of kv pairs: " + msgs.size());
            performance.averageLatency();
            performance.averageGetLatency();
            performance.averagePutLatency();
            System.out.println(" ");
            performance.Shutdown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}