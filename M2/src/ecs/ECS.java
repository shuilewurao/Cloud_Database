package ecs;

import app_kvECS.IECSClient;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import shared.HashingFunction.MD5;
import shared.messages.KVMessage;

import java.io.*;
import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ECS implements IECSClient {

    private Logger logger = Logger.getRootLogger();

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

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
    private static ZK ZKAPP = new ZK();
    private ZooKeeper zk;
    private static final String ZK_HOST = "localhost";
    private static final int ZK_TIMEOUT = 2000;
    private int ZK_PORT;
    private static final String ZK_ROOT_PATH = "/root";
    private static final String ZK_SERVER_PATH = "/server";
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    private static final String PWD = System.getProperty("user.dir");
    private static final String RUN_SERVER_SCRIPT = "script.sh";
    private static final String ZK_START_CMD = "zookeeper-3.4.11/bin/zkServer.sh start";
    private static final String ZK_STOP_CMD = "zookeeper-3.4.11/bin/zkServer.sh stop";
    private static final String WHITESPACE = " ";


    /**
     * @param configFilePath
     * @throws IOException
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
            System.exit(1);
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

        zk = ZKAPP.connect();

        logger.info("[ECS] ZooKeeper started!" + ZK_HOST);

        logger.info("[ECS] Initializing Logical Hash Ring...");

        hashRing = new ECSHashRing();

        logger.info("[ECS] New Logical Hash Ring with size: " + hashRing.getSize());

    }

    @Override
    public boolean start() throws Exception {
        // filter current activeNodes:

        Collection<ECSNode> toStart = new ArrayList<>();

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = (ECSNode) entry.getValue();
            if (n.getServerStateType().equals(KVMessage.ServerStateType.IDLE)) {
                toStart.add(n);
            } else {
                logger.debug("[ECS] " + n.getNodeName() + " is not in IDLE state!");
            }
        }

        // call start on Node using script.sh:
        // server <port> <cacheSize> <cacheStrategy>
        // java -jar $1/m2-server.jar $2 $3 $4  > "$1/logs/Server_$2.log" 2>&1

        Process proc;
        String script_path = PWD + "/" + RUN_SERVER_SCRIPT;


        for (ECSNode n : toStart) {

            String cmd_input = PWD + WHITESPACE + n.getNodePort() + WHITESPACE + n.getCacheSize() + WHITESPACE + n.getReplacementStrategy();

            logger.info("[ECS] Running Server starting cmd: " + script_path + WHITESPACE + cmd_input);
            Runtime run = Runtime.getRuntime();
            try {
                proc = run.exec(script_path + WHITESPACE + cmd_input);

                logger.info("[ECS] Setting " + n.getNodeName() + " to STARTED state!");

                hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.STARTED);
            } catch (IOException e) {
                logger.error("[ECS] Error running cmd: " + e);
                e.printStackTrace();
            }
        }

        return true;
    }

    @Override
    public boolean stop() throws Exception {

        Collection<ECSNode> allRunning = new ArrayList<>();

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();
            if (n.getServerStateType().equals(KVMessage.ServerStateType.STARTED)) {
                allRunning.add(n);
            } else {
                logger.debug("[ECS] " + n.getNodeName() + " is not in IDLE state!");
            }
        }

        for (ECSNode n : allRunning) {

            logger.info("[ECS] Setting " + n.getNodeName() + " to STOPPED state!");

            hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.STOPPED);
        }

        return false;
    }

    @Override
    public boolean shutdown() throws Exception {

        // for each active node, disconnect them? call KVStore??

        try {
            for (Map.Entry<String, ECSNode> entry : availableServers.entrySet()) {

                logger.info("[ECS] Setting " + entry.getValue().getNodeName() + " to STOPPED state!");
                entry.getValue().setServerStateType(KVMessage.ServerStateType.STOPPED);
            }

            for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
                hashRing.getNodeByName(entry.getValue().name).setServerStateType(KVMessage.ServerStateType.STOPPED);
                if (hashRing.removeNode(entry.getValue()) == null)

                availableNodeKeys.add(entry.getValue().name);
            }

            ZKAPP.close();

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
                    logger.error("[ECS] ZooKeeper cannot stop!");
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                logger.error("[ECS] ZooKeeper cannot stop! " + e);
                System.exit(1);
            }

            return true;
        } catch (Exception e) {
            logger.error("[ECS] Error shutting down... " + e);
            e.printStackTrace();
        }

        return false;
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

            assert rNode.getNodeHash() != null;

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

            if (zk.exists(ZK_ROOT_PATH, true) == null) { // Stat checks the path of the znode
                ZKAPP.create(ZK_ROOT_PATH, data);
            }

            if (zk.exists(ZK_SERVER_PATH, true) == null) { // Stat checks the path of the znode
                ZKAPP.create(ZK_SERVER_PATH, data);
            }

            for (int i = 0; i < count; ++i) {

                IECSNode node = addNode(cacheStrategy, cacheSize);
                result.add(node);

                byte[] metaData = SerializationUtils.serialize(node.getMetaData().getHost());


                String nodePath = ZK_SERVER_PATH + "/" + node.getNodeName();

                if (zk.exists(nodePath, true) == null) {
                    ZKAPP.create(nodePath, metaData);
                } else {
                    ZKAPP.update(nodePath, metaData);

                }
            }

        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        assert result.size() != 0;

        // call start() to start all the nodes
        try {
            start();
        } catch (Exception e) {
            logger.error("[ECS] Error starting nodes: " + e);
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {


        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        logger.info("[ECS] Removing nodes from Hash Ring...");

        for (String name : nodeNames) {
            logger.debug("[ECS] Removing node: " + name);

            assert name != null;

            if (availableNodeKeys.contains(name)) {
                ECSNode node = availableServers.get(name);
                assert node != null;
                /*
                Transfer meta data??
                 */

                // TODO

                // Remove from ring??

                try {
                    String[] newHashRange = hashRing.removeNode(node);

                    logger.debug("[ECS] New hash range: " + newHashRange[0] + " : " + newHashRange[1]);

                    // Adding back to available servers
                    availableNodeKeys.add(name);

                    // Set new state??
                    availableServers.get(name).setServerStateType(KVMessage.ServerStateType.STOPPED);
                } catch (Exception e) {
                    logger.error("[ECS] Error removing nodes!" + e);
                    e.printStackTrace();
                    return false;
                }
            } else {

                logger.error("[ECS] node is not in Hash Ring: " + name);

            }
        }
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {

        logger.info("[ECS] getting nodes...");

        Map<String, IECSNode> result = new HashMap<String, IECSNode>();

        assert hashRing.getSize() != 0;


        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode node = entry.getValue();
            result.put(node.getNodeName(), (IECSNode) node);

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
        return MD5.HashFromHostAddress(host, port);

    }


    /**
     * Synch changes on hashring with ZK
     */
    // TODO
    private boolean updateMetadata() {

        // return ZooKeeperUtils.update(ZooKeeperUtils.ZK_METADATA_ROOT, convertHashRingToJSON()().getBytes);
        return true;

    }

    // TODO
    /*
    public String convertHashRingToJSON() {
        List<ECSNode> activeNodes = nodeTable.values().stream()
                .map(n -> (ECSNode) n)
                .filter(n -> n.getStatus().equals(ECSNode.ECSNodeFlag.ACTIVE))
                .map(RawECSNode::new)
                .collect(Collectors.toList());

        return new Gson().toJson(activeNodes);
    }

     */

    // TODO: restore mechanism
}
