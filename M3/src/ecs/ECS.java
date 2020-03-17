package ecs;

import app_kvECS.IECSClient;
import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import shared.Constants;
import shared.HashingFunction.MD5;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECS implements IECSClient, Watcher {

    private Logger logger = Logger.getRootLogger();

    private static final String SERVER_JAR = "m3-server.jar";
    // Assumes that the jar file is located at the same dir on the remote server
    private static final String JAR_PATH = new File(System.getProperty("user.dir"), SERVER_JAR).toString();

    private HashMap<String, ECSNode> availableServers;


    // Logical Hash Ring object managing the ECSNodes
    private ECSHashRing hashRing;

    /*
    ZooKeeper instance
     */
    private static ZK ZKAPP = new ZK();
    private ZooKeeper zk;
    public static final String ZK_HOST = "127.0.0.1";
    //public static int ZK_PORT = 2181;
    public static final String ZK_SERVER_PATH = "/server";
    public static final String ZK_HASH_TREE = "/metadata";
    public static final String ZK_OP_PATH = "/op";

    private static final String PWD = System.getProperty("user.dir");
    //private static final String RUN_SERVER_SCRIPT = "script.sh";
    private static final String ZK_START_CMD = "zookeeper-3.4.11/bin/zkServer.sh start";
    //private static final String ZK_STOP_CMD = "zookeeper-3.4.11/bin/zkServer.sh stop";


    /**
     * @param configFilePath path to confid file ecs.config
     * @throws IOException exceptions
     */

    public ECS(String configFilePath) throws IOException {

        logger.info("[ECS] Starting new ECS...");

        String cmd = PWD + "/" + ZK_START_CMD;

        try {
            Process process = Runtime.getRuntime().exec(cmd);

            StringBuilder output = new StringBuilder();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            int exitVal = process.waitFor();
            if (exitVal == 0) {
                logger.info("[ECS] cmd success: " + cmd);
                logger.info(output);
                System.out.println("Success!");
                System.out.println(output);
            } else {
                logger.error("[ECS] cmd abnormal: " + cmd);
                logger.error("[ECS] ZooKeeper cannot start!");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            logger.error("[ECS] ZooKeeper cannot start! " + e);
            e.printStackTrace();
            System.exit(1);
        }

        /*
        Parse the ecs.config file
         */

        logger.info("[ECS] Parsing new ecs.config file...");

        try {
            BufferedReader reader = new BufferedReader(new FileReader(configFilePath));

            String line;

            availableServers = new HashMap<>();

            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\\s+");
                if (tokens.length != 3) {
                    logger.error("[ECS] Error! Invalid number of arguments!\n    Number of arguments: " + tokens.length + "\nUsage: Server <port> <cacheSize> <strategy>!");
                    System.exit(1);
                }

                String name = tokens[0];
                String host = tokens[1];
                int port = Integer.parseInt(tokens[2]);

                /*
                Create new node for every server
                 */

                logger.info("[ECS] creating new node...");

                try {
                    addingAvailableServerNodes(name, host, port);
                } catch (Exception e) {
                    logger.error("[ECS] Error! Cannot create node: " + name);
                    logger.error("[ECS]" + e);
                }

                logger.info("[ECS] New node added to available servers: " + name);

            }
            reader.close();

            assert availableServers != null;
            //availableNodeKeys = new ArrayList<>(availableServers.keySet());

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("[ECS] Cannot open file: " + configFilePath);
            logger.error("[ECS] Error!" + e);
        }

        /*

            new ZooKeeper instance

         */
        logger.info("[ECS] Starting new ZooKeeper...");

        zk = ZKAPP.connect();

        logger.info("[ECS] ZooKeeper started!" + ZK_HOST);

        logger.info("[ECS] Initializing Logical Hash Ring...");

        // TODO: initial state of hash ring?
        hashRing = new ECSHashRing();

        pushHashRingInTree();
        pushHashRingInZnode();

        logger.info("[ECS] New Logical Hash Ring with size: " + hashRing.getSize());

    }

    public void start_script(ECSNode node) {
        String javaCmd = String.join(" ",
                "java -jar",
                JAR_PATH,
                String.valueOf(node.getNodePort()),
                String.valueOf(node.getCacheSize()),
                node.getReplacementStrategy());
        String sshCmd = "ssh -o StrictHostKeyChecking=no -n " + "localhost" + " nohup " + javaCmd +
                "   > " + PWD + "/logs/Server_" + node.getNodePort() + ".log &";
        try {
            Runtime.getRuntime().exec(sshCmd);
            Thread.sleep(100);
        } catch (IOException | InterruptedException e) {
            logger.error("[ECS] error running cmd " + sshCmd + " " + e);
            e.printStackTrace();
        }
    }

    @Override

    public boolean start() throws KeeperException, InterruptedException {
        List<ECSNode> toStart = new ArrayList<>();
        CountDownLatch sig = new CountDownLatch(hashRing.getSize());

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();

            // Only start STOPPED nodes
            if (n.getServerStateType() == IKVServer.ServerStateType.STOPPED) {
                logger.debug("[ECS] node to start: " + n.name);

                String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;

                broadcast(msgPath, IECSNode.ECSNodeFlag.START.name(), sig);

                n.setFlag(ECSNodeMessage.ECSNodeFlag.START);
                n.setServerStateType(IKVServer.ServerStateType.STARTED);

                toStart.add(n);
            }
        }

        sig.countDown();

        for (ECSNode n : toStart) {
            if (hashRing.isReplicable()) {

                List<ECSDataReplication> toReplicate = new ArrayList<>();

                ECSNode senderCoordinator = this.hashRing.getNodeByServerName(n.name);
                String[] responsibleRange = this.hashRing.getResponsibleHashRange(senderCoordinator);
                assert responsibleRange[1].equals(senderCoordinator.getNodeHash());

                toReplicate.add(new ECSDataReplication(senderCoordinator, n, new String[]{responsibleRange[0], n.getNodeHash()}));

                Collection<ECSNode> replicas = this.hashRing.getReplicas(senderCoordinator);
                //replicas.add(senderCoordinator);

                for (ECSNode r : replicas) {
                    ECSNode lastNode = hashRing.getLastReplica(r);
                    lastNode = hashRing.getNextNode(lastNode.name);
                    String[] deleteRange = hashRing.getLastReplica(lastNode).getNodeHashRange();
                    toReplicate.add(new ECSDataReplication(r, deleteRange));
                }

                for (ECSDataReplication replication : toReplicate) {
                    replication.start(zk);
                }

            } else {
                logger.warn("[ECS] Hash ring size less than 3!");
            }
        }

        /*
        if (toStart.size() == 0) {

            List<String> children = zk.getChildren(ZK_SERVER_PATH, false);
            for (String child : children) {
                String data = new String(ZK.read(ZK_SERVER_PATH + "/" + child));

                logger.debug("[ECS] data path: " + ZK_SERVER_PATH + "/" + child);

                logger.debug("[ECS] persisted data: " + data);

                String[] tokens = data.split("\\" + Constants.DELIMITER);

                if (tokens.length > 6) {
                    String name = tokens[1];
                    String host = tokens[2];
                    int port = Integer.parseInt(tokens[3]);
                    int cacheSize = Integer.parseInt(tokens[4]);
                    String cacheStrategy = tokens[5];

                    assert child.equals(name);

                    ECSNode node = new ECSNode(name, host, port);
                    node.setCacheSize(cacheSize);
                    node.setReplacementStrategy(cacheStrategy);

                    hashRing.addNode(node);

                    toStart.add(node);
                } else {
                    logger.warn("[ECS] Not enough data to start " + child);
                }
            }
        }
        */
        pushHashRingInTree();
        pushHashRingInZnode();

        return true;
    }


    @Override
    public boolean shutdown() throws Exception {
        // for each active node, disconnect them? call KVStore??

        CountDownLatch sig = new CountDownLatch(hashRing.getSize());

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {

            logger.info("[ECS] Setting " + entry.getValue().getNodeName() + " to SHUTDOWN state!");
            ECSNode n = entry.getValue();
            n.setServerStateType(IKVServer.ServerStateType.SHUT_DOWN);

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;
            broadcast(msgPath, IECSNode.ECSNodeFlag.SHUT_DOWN.name(), sig);
            n.shutdown();
            availableServers.put(n.getNodeName(), n);
        }

        hashRing.removeAllNode();
        pushHashRingInZnode();
        return pushHashRingInTree();
    }

    @Override
    public boolean stop() throws Exception {

        CountDownLatch sig = new CountDownLatch(hashRing.getSize());

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {

            logger.info("[ECS] Setting " + entry.getValue().getNodeName() + " to STOPPED state!");
            ECSNode n = entry.getValue();
            n.setServerStateType(IKVServer.ServerStateType.STOPPED);
            n.setFlag(ECSNodeMessage.ECSNodeFlag.STOP);

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;
            broadcast(msgPath, IECSNode.ECSNodeFlag.STOP.name(), sig);

        }
        pushHashRingInZnode();
        return pushHashRingInTree();
    }


    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize, boolean isSole) {

        if (availableServers.size() == 0) {
            logger.warn("[ECS] No available servers!");
            return null;
        }

        Map.Entry<String, ECSNode> entry = availableServers.entrySet().iterator().next();

        String key = entry.getKey();

        ECSNode rNode = entry.getValue();

        availableServers.remove(key);

        rNode.init(cacheSize, cacheStrategy);

        hashRing.addNode(rNode);

        if (isSole) {
            migrateAddedServer(rNode);
            pushHashRingInTree();
            pushHashRingInZnode();
        }

        return rNode;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {


        logger.info("[ECS] Initiating storage service...");

        List<IECSNode> result = new ArrayList<>();

        if (availableServers.size() == 0) {
            logger.info("[ECS] No storage service available...");

            return null;
        }

        try {
            if (zk.exists(ZK_SERVER_PATH, false) == null) {
                ZKAPP.create(ZK_SERVER_PATH, "".getBytes());
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("[ECS] Error when creating zk server! " + e);
            e.printStackTrace();
        }

        for (int i = 0; i < count; ++i) {
            //System.out.println(i);
            ECSNode node = addNode(cacheStrategy, cacheSize, false);

            result.add(node);

            String znodePath = ZK_SERVER_PATH + "/" + node.getNodePort();

            try {

                if (zk.exists(znodePath, false) == null) {

                    ZKAPP.create(znodePath, "".getBytes());

                } else {
                    ZK.update(znodePath, "".getBytes());
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        pushHashRingInTree();
        pushHashRingInZnode();

        setupNodes(count, cacheStrategy, cacheSize);

        return null;

    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {

        CountDownLatch sig = new CountDownLatch(hashRing.getSize());

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {

            ECSNode n = entry.getValue();

            if (n.getServerStateType().equals(IKVServer.ServerStateType.STOPPED)) {
                logger.debug("[ECS] setting up " + n.name);
                String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;

                broadcast(msgPath, IECSNode.ECSNodeFlag.INIT.name(), sig);

                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    logger.error("[ECS] Error! " + e);
                    e.printStackTrace();
                }
                start_script(n);
            }
        }
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {

        return false;
    }

    public void broadcast(String msgPath, String msg, CountDownLatch sig) {

        try {
            if (zk.exists(msgPath, this) == null) {
                ZKAPP.create(msgPath, msg.getBytes());
            } else {
                logger.warn("[ECS] " + msgPath + " already exists... updating and deleting children...");
                ZK.update(msgPath, msg.getBytes());
            }

            if (zk.exists(msgPath, this) == null) {
                sig.countDown();
                logger.debug("[ECS] Unable to create path " + msgPath);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("[ECS] Exception sending ZK msg at " + msgPath + ": " + e);
            e.printStackTrace();
        }
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        logger.debug("[ECS]: Removing nodes: " + nodeNames);

        // if only one node on ring
        CountDownLatch sig = new CountDownLatch(hashRing.getSize());

        if (hashRing.getSize() == 1) {
            logger.info("[ECS]: Only one node in hash ring!");
            assert nodeNames.size() == 1;
            for (String name : nodeNames) {

                assert name != null;

                ECSNode n = hashRing.getNodeByServerName(name);

                if (n == null) {
                    logger.error("[ECS] node is not in Hash Ring: " + name);
                    continue;
                }

                String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;
                broadcast(msgPath, IECSNode.ECSNodeFlag.SHUT_DOWN.name(), sig);

                n.shutdown();
                hashRing.removeNode(n);
                availableServers.put(n.getNodeName(), n);
            }
            return true;
        }

        boolean ret = true;
        List<ECSNode> toRemove = new ArrayList<>();

        for (String name : nodeNames) {

            assert name != null;

            ECSNode n = hashRing.getNodeByServerName(name);


            if (n == null) {
                logger.error("[ECS] node is not in Hash Ring: " + name);
                continue;
            }

            n.setFlag(ECSNodeMessage.ECSNodeFlag.KV_TRANSFER);
            logger.debug("[ECS] adding node to remove: " + name);
            logger.debug("[ECS] Remove node hash: " + n.getNodeHashRange()[0] + ":" + n.getNodeHashRange()[1]);

            if (hashRing.isReplicable()) {
                Collection<ECSNode> replicas = hashRing.getReplicas(n);
                CountDownLatch sig1 = new CountDownLatch(replicas.size());
                for (ECSNode coordinator : replicas) {
                    ECSDataReplication dataReplicate = new ECSDataReplication(coordinator, coordinator.getNodeHashRange());

                    try {
                        dataReplicate.start(zk);
                    } catch (InterruptedException | KeeperException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                logger.warn("[ECS] Hash ring size less than 3!");
            }

            toRemove.add(n);

            //String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() +"/op";
            //broadcast(msgPath, IECSNode.ECSNodeFlag.KV_TRANSFER.name(), sig);

            //ECSNode node = availableServers.get(name);

            // assert node != null;

        }

        pushHashRingInZnode();
        ret &= pushHashRingInTree();

        if (migrateRemovedServers(toRemove))
            logger.info("[ECS] data transfer success!");
        else
            logger.info("[ECS] no data transferred!");

        for (ECSNode n : toRemove) {
            // TODO: check transfer finish
            logger.debug("[ECS] removing: " + n.getNodeName());

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;
            broadcast(msgPath, IECSNode.ECSNodeFlag.SHUT_DOWN.name(), sig);

            n.shutdown();
            hashRing.removeNode(n);
            availableServers.put(n.getNodeName(), n);
        }
        pushHashRingInZnode();
        ret &= pushHashRingInTree();

        return ret;
    }

    private boolean migrateRemovedServers(Collection<ECSNode> nodes) {

        for (ECSNode node : nodes) {
            ECSNode target = findNextServingNode(node);

            if (target == null) {
                logger.warn("[ECS] No available servers for data transfer!");
                return false;
            }
            assert !node.name.equals(target.name);
            transferData(node, target, node.getNodeHashRange());
        }
        return true;
    }

    private boolean migrateAddedServer(ECSNode node) {
        ECSNode source = findNextServingNode(node);
        if (source != null) {
            assert !node.name.equals(source.name);
            return transferData(source, node, node.getNodeHashRange());

        } else {
            logger.warn("[ECS] No data to transfer to node " + node.name);
        }
        return true;
    }

    private ECSNode findNextServingNode(ECSNode n) {

        ECSNode next = hashRing.getNextNode(n.getNodeHash());
        int cnt = hashRing.getSize();

        while (cnt > 0) {
            if (next.getFlag() == ECSNodeMessage.ECSNodeFlag.START) {
                return next;
            }
            next = hashRing.getNextNode(next.getNodeHash());
            cnt--;
        }
        return null;

    }

    private boolean transferData(ECSNode from, ECSNode to, String[] hashRange) {
        assert from != null;
        assert to != null;
        assert hashRange.length == 2;

        logger.debug("[ECS] transferring " + from.getNodeName() + " to " + to.getNodeName());

        try {
            CountDownLatch sig = new CountDownLatch(1);
            String msgPath = ZK_SERVER_PATH + "/" + from.getNodePort() + ZK_OP_PATH;

            String to_msg = IECSNode.ECSNodeFlag.KV_TRANSFER.name() +
                    Constants.DELIMITER + to.getNodePort()
                    + Constants.DELIMITER + hashRange[0]
                    + Constants.DELIMITER + hashRange[1];

            broadcast(msgPath, to_msg, sig);

            logger.info("[ECS] Confirmed receiver node " + to.getNodeName());
            logger.info("[ECS] Confirmed sender node " + from.getNodeName());

            // Start listening sender's progress
            String toPath = ZK_SERVER_PATH + "/" + to.getNodePort() + ZK_OP_PATH;
            String fromPath = ZK_SERVER_PATH + "/" + from.getNodePort() + ZK_OP_PATH;

            while (true) {

                String fromMsg = new String(ZK.readNullStat(fromPath));
                //String toMsg = new String(ZK.readNullStat(toPath));
                //if (fromMsg.equals(IECSNode.ECSNodeFlag.TRANSFER_FINISH.name()) && toMsg.equals(IECSNode.ECSNodeFlag.TRANSFER_FINISH.name())) {
                if (fromMsg.equals(IECSNode.ECSNodeFlag.TRANSFER_FINISH.name())) {
                    if (!(zk.exists(toPath, false) == null))
                        ZK.delete(toPath);
                    if (!(zk.exists(fromPath, false) == null))
                        ZK.delete(fromPath);
                    break;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {

        logger.info("[ECS] getting nodes...");

        Map<String, IECSNode> result = new HashMap<>();

        assert hashRing.getSize() != 0;


        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode node = entry.getValue();
            result.put(node.getNodeName(), node);

            logger.debug("[ECS] node: " + node.getNodeName());

        }

        return result;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        assert hashRing.getSize() != 0;

        ECSNode node;
        try {
            BigInteger keyHash = MD5.HashInBI(Key);
            node = hashRing.getNodeByHash(keyHash);
        } catch (Exception e) {
            logger.error("[ECS]");
            e.printStackTrace();
            return null;
        }

        return node;

    }

    public void addingAvailableServerNodes(String name, String host, int port) {

        if (availableServers.containsKey(name)) {
            logger.error("[ECS] Error! Server: " + name + " already added!\n");
            System.exit(1);
        }
        ECSNode node = new ECSNode(name, host, port);

        availableServers.put(name, node);
    }
/*
    public BigInteger mdHashServer(String host, int port) {
        return MD5.HashFromHostAddress(host, port);
    }
*/


    /*
     This pushes the latest hashRing to Zookeeper's metadata directory.
     Note: only server nodes added to the hash ring
     */
    private boolean pushHashRingInTree() {
        try {

            if (zk.exists(ZK_HASH_TREE, false) == null) {
                ZKAPP.create(ZK_HASH_TREE, hashRing.getHashRingJson().getBytes());  // NOTE: has to be persistent
            } else {
                ZK.update(ZK_HASH_TREE, hashRing.getHashRingJson().getBytes());
            }
        } catch (InterruptedException e) {
            logger.error("[ECS] Interrupted! " + e);
            e.printStackTrace();
            return false;
        } catch (KeeperException e) {
            logger.error("[ECS] Error! " + e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private boolean pushHashRingInZnode() {

        if (hashRing.getSize() == 0) {
            logger.warn("[ECS] Empty hash ring!");
            return true;
        }

        try {

            if (zk.exists(ZK_SERVER_PATH, false) == null) {
                ZKAPP.create(ZK_SERVER_PATH, "".getBytes());  // NOTE: has to be persistent
            } else {


                for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {

                    ECSNode n = entry.getValue();

                    String zDataPath = ZK_SERVER_PATH + "/" + n.getNodePort() + "/metadata";

                    if (zk.exists(zDataPath, false) == null) {
                        ZKAPP.create(zDataPath, n.getNodeJson().getBytes());  // NOTE: has to be persistent
                    } else {
                        ZK.update(zDataPath, n.getNodeJson().getBytes());
                    }
                }

            }
        } catch (InterruptedException e) {
            logger.error("[ECS] Interrupted! " + e);
            e.printStackTrace();
            return false;
        } catch (KeeperException e) {
            logger.error("[ECS] Error! " + e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean ifAllValidServerNames(Collection<String> nodeNames) {
        if (nodeNames.size() <= hashRing.getSize()) {
            return true;
        } else {
            logger.error("[ECSClient] Invalid # of nodes to remove.");
            return false;
        }
    }


    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
