package ecs;

import app_kvECS.IECSClient;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import shared.messages.KVMessage;

import java.io.*;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import java.security.MessageDigest;

public class ECS implements IECSClient {

    private Logger logger = Logger.getRootLogger();

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
    private static final String DELIMITER = "+";
    private static final String HASH_DELIMITER = ":";

    private OutputStream output;
    private InputStream input;

    private boolean running;

    private HashMap<String, ECSNode> availableServers;


    // Logical Hash Ring object managing the ECSNodes
    private ECSHashRing hashRing;
    private List<String> availableNodeKeys;

    /*
    ZooKeeper instance
     */
    private ZooKeeper zk;
    private static final String ZK_HOST = "localhost";
    private static final int ZK_TIMEOUT = 2000;
    private int ZK_PORT;
    private static final String ZK_ACTIVE_PATH = "/root";
    private static final String ZK_SERVER_PATH = "/server";

    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * @param configFilePath
     * @throws IOException
     */

    public ECS(String configFilePath) throws IOException {

        logger.info("[ECS] Starting new ECS...");

        String cmd = "/Users/xuanchen/Desktop/ece419/M2/zookeeper-3.4.11/bin/zkServer.sh start";

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
            logger.error("[ECS] ZooKeeper cannot start!" + e);
        }

        /*
        Parse the ecs.config file
         */

        logger.info("[ECS] Parsing new ecs.config file...");

        try {
            BufferedReader reader = new BufferedReader(new FileReader(configFilePath));

            String line = reader.readLine().trim();

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
            availableNodeKeys = new ArrayList<>(availableServers.keySet());

        } catch (IOException e) {
            e.printStackTrace();
            logger.error("[ECS] Cannot open file: " + configFilePath);
            logger.error("[ECS] Error!" + e);
        }

        /*

            new ZooKeeper instance

         */
        logger.info("[ECS] Starting new ZooKeeper...");

        zk = new ZooKeeper(ZK_HOST, ZK_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });

        try {
            connectedSignal.await();
        } catch (InterruptedException e) {
            logger.error("[ECS] ZooKeeper connection error!" + e);
        }

        logger.info("[ECS] ZooKeeper started!" + ZK_HOST);

        logger.info("[ECS] Initializing Logical Hash Ring...");

        hashRing = new ECSHashRing();

        logger.info("[ECS] New Logical Hash Ring with size: " + hashRing.getSize());

    }

    @Override
    public boolean start() throws Exception {
        // TODO

        // start every active node??
        // call script?

        return false;
    }

    @Override
    public boolean stop() throws Exception {
        // TODO

        // for each active node, stop them? call KVStore??
        return false;
    }

    @Override
    public boolean shutdown() throws Exception {
        // TODO

        // for each active node, disconnect them? call KVStore??
        return true;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {

        /*
        Randomly pick one of the idle servers in the repository
         */
        Random r = new Random();

        String rKey = availableNodeKeys.get(r.nextInt(availableNodeKeys.size()));

        // Get a random node from available servers
        ECSNode rNode = availableServers.get(rKey);

        rNode.setReplacementStrategy(cacheStrategy);
        rNode.setCacheSize(cacheSize);

        // Remove the used randomKey
        availableNodeKeys.remove(rKey);

        // Server should be in STOPPED state
        if (rNode.getServerStateType().equals(KVMessage.ServerStateType.STOPPED)) {
            rNode.setServerStateType(KVMessage.ServerStateType.IDLE);
            logger.info("[ECS] Adding node: " + rNode.getNodeName());

            BigInteger serverHash = null;
            try {
                serverHash = mdHashServer(rNode.getNodeHost(), rNode.getNodePort());
            } catch (NoSuchAlgorithmException e) {
                logger.error("[ECS] Error!" + e);
                e.printStackTrace();
            }

            rNode.setNodeHash(serverHash);

            logger.info("[ECS] Adding node to Hash Ring...");
            hashRing.addNode(rNode);


        } else {
            logger.error("[ECS] Trying to START a non STOPPED node: " + rNode.getNodeName());
            logger.error("[ECS] Program exit...");
            System.exit(1);
        }
        return rNode;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {

        logger.info("[ECS] Initiating storage service...");

        if (availableServers.size() == 0) {
            logger.info("[ECS] No storage service available...");

            return null;
        }

        return setupNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {

        logger.info("[ECS] Initiating setting up nodes...");

        Collection<IECSNode> result = new ArrayList<IECSNode>();

        byte[] data = "".getBytes();

        try {

            if (zk.exists(ZK_ACTIVE_PATH, true) == null) { // Stat checks the path of the znode
                zk.create(ZK_ACTIVE_PATH, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            if (zk.exists(ZK_SERVER_PATH, true) == null) { // Stat checks the path of the znode
                zk.create(ZK_SERVER_PATH, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            for (int i = 0; i < count; ++i) {

                // place on ring???
                IECSNode node = addNode(cacheStrategy, cacheSize);
                result.add(node);

                byte[] metaData = SerializationUtils.serialize((Serializable) node.getMetaData());

                String nodePath = ZK_SERVER_PATH + "/" + node.getNodeName();

                if (zk.exists(nodePath, true) == null) {
                    zk.create(nodePath, metaData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } else {
                    zk.setData(nodePath, metaData, zk.exists(nodePath, true).getVersion());
                    List<String> children = zk.getChildren(nodePath, false);
                    for (int j = 0; j < children.size(); ++j)
                        zk.delete(nodePath + "/" + children.get(i), zk.exists(nodePath + "/" + children.get(i), false).getVersion());
                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        assert result.size() != 0;
        return result;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO

        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        logger.info("[ECS] Removing nodes from Hash Ring...");

        for (String name : nodeNames) {

            assert name != null;

            if (availableNodeKeys.contains(name)) {
                logger.error("[ECS] node is not in Hash Ring: " + name);
            } else {
                logger.info("[ECS] Removing node: " + name);

                try {
                    ECSNode node = availableServers.get(name);

                    /*
                    Transfer meta data??
                     */

                    // TODO

                    // Remove from ring??

                    hashRing.removeNode(node);

                    // Adding back to available servers
                    availableNodeKeys.add(name);

                    // Set new state??
                    availableServers.get(name).setServerStateType(KVMessage.ServerStateType.STOPPED);
                } catch (Exception e) {
                    logger.error("[ECS] Error removing nodes!" + e);
                    e.printStackTrace();
                    return false;
                }

            }
        }
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {

        Map<String, IECSNode> result = new HashMap<String, IECSNode>();

        assert hashRing.getSize() != 0;

        List<ECSNode> activeNodes = hashRing.getActiveNodes();

        for (ECSNode node : activeNodes) {
            result.put(node.getNodeName(), (IECSNode) node);
        }

        return result;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        assert hashRing.getSize() != 0;

        ECSNode node;
        try {
            BigInteger keyHash = mdHashKey(Key);
            node = hashRing.getNodeByHash(keyHash);
        } catch (Exception e) {
            logger.error("[ECS]");
            e.printStackTrace();
            return null;
        }

        return node;

    }

    /*


     *****HELPER FUNCTIONS*****


     */

    public void addingAvailableServerNodes(String name, String host, int port) {

        if (availableServers.containsKey(name)) {
            logger.error("[ECS] Error! Server: " + name + " already added!\n");
            System.exit(1);
        }
        ECSNode node = new ECSNode(name, host, port);

        availableServers.put(name, node);
    }

    public BigInteger mdHashServer(String host, int port) throws NoSuchAlgorithmException {

        assert host != null;
        assert port != -1;

        String val = host + HASH_DELIMITER + port;

        return mdHash(val);

    }

    public BigInteger mdHashKey(String key) throws NoSuchAlgorithmException {

        assert key != null;

        return mdHash(key);

    }

    public BigInteger mdHash(String val) throws NoSuchAlgorithmException {

        MessageDigest md = null;

        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.error("[ECS] Error!" + e);
            e.printStackTrace();
        }

        assert md != null;
        md.reset();

        md.update(val.getBytes());

        return new BigInteger(1, md.digest());
    }
}
