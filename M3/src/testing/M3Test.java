package testing;

import client.KVStore;
import ecs.ECS;
import ecs.ECSHashRing;
import ecs.ECSNode;
import ecs.IECSNode;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class M3Test extends TestCase {

    public void test01Replication() throws Exception {

        String key = "a";
        String value = "bar";

        ECS ecs = new ECS("./ecs.config");

        ecs.addNodes(3, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        KVStore client = new KVStore("localhost", 50000);
        client.connect();
        Thread.sleep(1000);

        client.put(key, value);

        Map<String, IECSNode> replicas = ecs.getNodes();

        for (Map.Entry<String, IECSNode> entry : replicas.entrySet()) {

            ECSNode n = (ECSNode) entry.getValue();

            KVStore newClient = new KVStore("localhost", n.getNodePort());
            newClient.connect();

            assert newClient.get(key).getValue().equals(value);
            newClient.disconnect();
        }

        client.disconnect();
        ecs.shutdown();
    }

    public void test02ReplicationAfterAddNode() throws Exception {

        int cnt = 0;
        String key = "a";
        String value = "bar";

        ECS ecs = new ECS("./ecs.config");

        ecs.addNodes(3, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        KVStore client = new KVStore("localhost", 50000);
        client.connect();
        Thread.sleep(1000);

        client.put(key, value);

        ecs.addNodes(2, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        Map<String, IECSNode> replicas = ecs.getNodes();

        for (Map.Entry<String, IECSNode> entry : replicas.entrySet()) {

            ECSNode n = (ECSNode) entry.getValue();

            KVStore newClient = new KVStore("localhost", n.getNodePort());
            newClient.connect();

            if (newClient.get(key).getValue().equals(value))
                cnt++;
            newClient.disconnect();
        }
        assert cnt == 3;
        client.disconnect();
        ecs.shutdown();
    }

    public void test03ReplicationAfterRemoveNode() throws Exception {

        int cnt = 0;
        String key = "a";
        String value = "bar";

        ECS ecs = new ECS("./ecs.config");

        ecs.addNodes(6, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        KVStore client = new KVStore("localhost", 50000);
        client.connect();
        Thread.sleep(1000);

        client.put(key, value);

        Collection<String> toRemove = new ArrayList<>();

        Map<String, IECSNode> replicas = ecs.getNodes();

        for (Map.Entry<String, IECSNode> entry : replicas.entrySet()) {

            ECSNode n = (ECSNode) entry.getValue();

            KVStore newClient = new KVStore("localhost", n.getNodePort());
            newClient.connect();

            if (newClient.get(key).getValue().equals(value) && n.getNodePort() != 50000) {
                toRemove.add(n.getName());
            }
            newClient.disconnect();
        }

        ecs.removeNodes(toRemove);

        for (Map.Entry<String, IECSNode> entry : replicas.entrySet()) {

            ECSNode n = (ECSNode) entry.getValue();

            KVStore newClient = new KVStore("localhost", n.getNodePort());
            newClient.connect();

            if (newClient.get(key).getValue().equals(value)) {
                cnt++;
            }
            newClient.disconnect();
        }

        assert cnt == 3;
        client.disconnect();
        ecs.shutdown();
    }

    public void test04NodeFailureDetection() throws Exception {

        ECS ecs = new ECS("./ecs.config");

        ecs.addNodes(3, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        Process p;

        String cmd = "./killNode.sh 50000";
        p = Runtime.getRuntime().exec(cmd);

        Thread.sleep(3000);

        assert ecs.getNodes().size() == 3;
        ecs.shutdown();

    }

    public void test05NodeFailureRecovery() throws Exception {

        ECS ecs = new ECS("./ecs.config");

        ecs.addNodes(3, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        Process p;

        String cmd = "./killNode.sh 50000";
        p = Runtime.getRuntime().exec(cmd);

        Thread.sleep(3000);

        assert ecs.getNodes().size() == 3;
        ecs.shutdown();
    }

    public void test06ReplicaGet() throws Exception {

        String key = "a";
        String value = "bar";

        ECS ecs = new ECS("./ecs.config");

        ecs.addNodes(3, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        KVStore client = new KVStore("localhost", 50000);
        client.connect();
        Thread.sleep(1000);

        client.put(key, value);

        Collection<String> toRemove = new ArrayList<>();

        Map<String, IECSNode> replicas = ecs.getNodes();

        for (Map.Entry<String, IECSNode> entry : replicas.entrySet()) {

            ECSNode n = (ECSNode) entry.getValue();

            KVStore newClient = new KVStore("localhost", n.getNodePort());
            newClient.connect();

            assert newClient.get(key).getValue().equals(value);
            newClient.disconnect();
        }
        client.disconnect();
        ecs.shutdown();
    }

    public void test07ReplicaPut() throws Exception {
        String key = "a";
        String value = "bar";

        ECS ecs = new ECS("./ecs.config");

        ecs.addNodes(3, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        KVStore client = new KVStore("localhost", 50000);
        client.connect();
        Thread.sleep(1000);

        client.put(key, value);


        Map<String, IECSNode> replicas = ecs.getNodes();

        for (Map.Entry<String, IECSNode> entry : replicas.entrySet()) {

            ECSNode n = (ECSNode) entry.getValue();
            //TODO
        }
        client.disconnect();
        ecs.shutdown();
    }

    public void test08HashRingAddingNode() {
        ECSHashRing hashRing = new ECSHashRing();

        ECSNode tmp = new ECSNode("server1", "localhost", 50001);

        hashRing.addNode(new ECSNode("server1", "localhost", 50001));
        hashRing.addNode(new ECSNode("server2", "localhost", 50002));

        assert hashRing.getReplicas(tmp).size() == 2;

        hashRing.addNode(new ECSNode("server3", "localhost", 50003));
        hashRing.addNode(new ECSNode("server4", "localhost", 50004));
        hashRing.addNode(new ECSNode("server5", "localhost", 50005));

        assert hashRing.getReplicas(tmp).size() == 2;

        hashRing.removeAllNode();
    }

    public void test09HashRingRemovingNode() {
        ECSHashRing hashRing = new ECSHashRing();

        ECSNode tmp = new ECSNode("server1", "localhost", 50001);

        hashRing.addNode(new ECSNode("server1", "localhost", 50001));
        hashRing.addNode(new ECSNode("server2", "localhost", 50002));
        hashRing.addNode(new ECSNode("server3", "localhost", 50003));
        hashRing.addNode(new ECSNode("server4", "localhost", 50004));
        hashRing.addNode(new ECSNode("server5", "localhost", 50005));

        Collection<ECSNode> replicas = hashRing.getReplicas(tmp);

        for (ECSNode n : replicas)
            hashRing.removeNode(n);

        assert hashRing.getReplicas(tmp).size() == 2;

        hashRing.removeAllNode();
    }

    public void test10Consistency() throws Exception {
        int cnt = 0;

        String key = "a";
        String value = "bar";
        String newValue = "barbar";

        ECS ecs = new ECS("./ecs.config");

        ecs.addNodes(3, "FIFO", 10);
        Thread.sleep(1000);

        ecs.start();

        KVStore client = new KVStore("localhost", 50000);
        client.connect();
        Thread.sleep(1000);

        client.put(key, value);

        Process p;
        String cmd = "./killNode.sh 50001";
        p = Runtime.getRuntime().exec(cmd);

        client.put(key, newValue);

        Thread.sleep(3000);

        Map<String, IECSNode> replicas = ecs.getNodes();

        for (Map.Entry<String, IECSNode> entry : replicas.entrySet()) {
            cnt++;
            ECSNode n = (ECSNode) entry.getValue();

            KVStore newClient = new KVStore("localhost", n.getNodePort());
            newClient.connect();

            assert newClient.get(key).getValue().equals(newValue);

            newClient.disconnect();
        }
        assert cnt == 3;
        client.disconnect();
        ecs.shutdown();
    }
}
