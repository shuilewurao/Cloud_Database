package shared;

import client.KVStore;
import ecs.ECS;
import ecs.IECSNode;
import org.apache.zookeeper.KeeperException;
import shared.messages.KVMessage;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.*;

public class RedundancyPerformance {

    private long totalGetLatency;
    private long totalPutLatency;

    private ECS ecs;
    private String CACHE_STRATEGY;
    private Integer CACHE_SIZE;
    private KVStore client;
    private Integer REQUEST_NUM;

    private String key = "myKey";
    private String value = "myValue";

    public RedundancyPerformance(int cacheSize, String strategy, int requestNum) throws Exception {
        this.totalGetLatency = 0;
        this.totalPutLatency = 0;

        this.CACHE_SIZE = cacheSize;
        this.CACHE_STRATEGY = strategy;

        this.REQUEST_NUM = requestNum;

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
            Thread.sleep(1000);
            ecs.start();
            Thread.sleep(1000);
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
        for (int i = 0; i < REQUEST_NUM; ++i) {

            KVMessage ret = this.client.put(this.key, this.value);
        }

        long end = System.nanoTime();
        this.totalPutLatency = end - start;

        /* ----- GET ----- */
        start = System.nanoTime();
        for (int i = 0; i < REQUEST_NUM; i++) {
            KVMessage ret = this.client.get(this.key);
            assertEquals(KVMessage.StatusType.GET_SUCCESS, ret.getStatus());
            assertEquals(this.value, ret.getValue());
        }
        end = System.nanoTime();
        this.totalGetLatency = end - start;

        this.client.disconnect();
        this.removeNodes();
        Thread.sleep(3000);
    }

    public void averageLatency() {
        float totalAvgLat = (this.totalGetLatency + this.totalPutLatency) / (float) REQUEST_NUM;
        System.out.println("\tTotal Average Latency: " + totalAvgLat / 1000000);
    }

    public void averagePutLatency() {
        float averagePutLat = this.totalPutLatency / (float) REQUEST_NUM;
        System.out.println("\tAverage PUT Latency: " + averagePutLat / 1000000);
    }

    public void averageGetLatency() {
        float averageGetLat = this.totalGetLatency / (float) REQUEST_NUM;
        System.out.println("\tAverage GET Latency: " + averageGetLat / 1000000);
    }

    public void Shutdown() throws Exception {
        boolean ret = this.ecs.shutdown();
        Thread.sleep(2000);
        assertTrue(ret);
    }

    public static void main(String[] args) {

        int cacheSize = 30;
        String cacheStrategy = "FIFO";
        int requestNum = 10000;

        try {
            RedundancyPerformance performance = new RedundancyPerformance(cacheSize, cacheStrategy, requestNum);
            performance.startTest();

            System.out.println("\n********** Latency in milliseconds **********");


            System.out.println("\t # of kv pairs: " + requestNum);
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