package ecs;

import app_kvECS.IECSClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import shared.Constants;
import shared.HashingFunction.MD5;
import shared.messages.KVMessage;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ECS implements IECSClient {

    private Logger logger = Logger.getRootLogger();

    private HashMap<String, ECSNode> availableServers;


    // Logical Hash Ring object managing the ECSNodes
    private ECSHashRing hashRing;
    private List<String> availableNodeKeys;

    private boolean INITIALIZED = false;

    /*
    ZooKeeper instance
     */
    private static ZK ZKAPP = new ZK();
    private ZooKeeper zk;
    public static final String ZK_HOST = "localhost";
    public static int ZK_PORT = 2181;
    private static final String ZK_ROOT_PATH = "/root";
    public static final String ZK_SERVER_PATH = "/server";
    public static final String ZK_HASH_TREE = "/metadata";

    private static final String PWD = System.getProperty("user.dir");
    private static final String RUN_SERVER_SCRIPT = "script.sh";
    private static final String ZK_START_CMD = "zookeeper-3.4.11/bin/zkServer.sh start";
    private static final String ZK_STOP_CMD = "zookeeper-3.4.11/bin/zkServer.sh stop";
    private static final String WHITESPACE = " ";

    public enum OPERATIONS {START, STOP, SHUT_DOWN, UPDATE, TRANSFER, INIT, STATE_CHANGE, KV_TRANSFER, TRANSFER_FINISH}


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
        pushHashRingInTree();

        logger.info("[ECS] New Logical Hash Ring with size: " + hashRing.getSize());

    }

    public void start_script() throws Exception {
        // filter current activeNodes:

        Collection<ECSNode> toStart = new ArrayList<>();

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();
            if (!n.getServerStateType().equals(KVMessage.ServerStateType.STARTED)) {
                toStart.add(n);
            } else {
                logger.debug("[ECS] " + n.getNodeName() + " is in STARTED state!");
            }
        }

        // call start on Node using script.sh:
        // server <servername> <zkhost> <zkport>
        // java -jar $1/m2-server.jar $2 $3 $4  > "$1/logs/Server_$2.log" 2>&1

        String script_path = PWD + "/" + RUN_SERVER_SCRIPT;

        for (ECSNode n : toStart) {

            String cmd_input = PWD + WHITESPACE + n.getNodeName() + WHITESPACE + ZK_HOST + WHITESPACE + ZK_PORT + WHITESPACE +  n.getNodeHost();

            logger.info("[ECS] Running Server starting cmd: " + script_path + WHITESPACE + cmd_input);
            Runtime run = Runtime.getRuntime();
            try {
                run.exec(script_path + WHITESPACE + cmd_input);

                hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.STOPPED);

                updateMetaData(hashRing.getNodeByName(n.name), OPERATIONS.INIT);

            } catch (IOException e) {
                logger.error("[ECS] Error running cmd: " + e);
                e.printStackTrace();
            }
        }

        pushHashRingInTree();
    }

    @Override

    public boolean start() throws Exception {
        // filter current activeNodes:

        Collection<ECSNode> toStart = new ArrayList<>();

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();
            if (n.getServerStateType().equals(KVMessage.ServerStateType.IDLE) || n.getServerStateType().equals(KVMessage.ServerStateType.STOPPED)) {
                toStart.add(n);
            }
        }

        if (toStart.size() == 0) {

            List<String> children = zk.getChildren(ZK_SERVER_PATH, false);
            for (String child : children) {
                String data = new String(ZK.read(ZK_SERVER_PATH + "/" + child));

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
                    node.setServerStateType(KVMessage.ServerStateType.STOPPED);

                    hashRing.addNode(node);

                    toStart.add(node);
                } else {
                    logger.warn("[ECS] Not enough data to start " + child);
                }

            }
        }

        // call start on Node using script.sh:
        // server <server_name> <zkHostName> <zkPort>
        // java -jar $1/m2-server.jar $2 $3 $4  > "$1/logs/Server_$2.log" 2>&1

        String script_path = PWD + "/" + RUN_SERVER_SCRIPT;

        for (ECSNode n : toStart) {

            String cmd_input = PWD + WHITESPACE + n.getNodeName() + WHITESPACE + ZK_HOST + WHITESPACE + ZK_PORT + WHITESPACE + n.getNodeHost();

            Runtime run = Runtime.getRuntime();
            try {
                if (!INITIALIZED) {
                    logger.info("[ECS] Running Server starting cmd: " + script_path + WHITESPACE + cmd_input);

                    run.exec(script_path + WHITESPACE + cmd_input);

                    logger.info("[ECS] Setting " + n.getNodeName() + " to STARTED state!");

                    hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.STOPPED);

                    updateMetaData(hashRing.getNodeByName(n.name), OPERATIONS.INIT);
                }

                n.setServerStateType(KVMessage.ServerStateType.STARTED);

                hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.STARTED);

                logger.debug("[ECS] type: " + hashRing.getNodeByName(n.name).getServerStateType().toString());

                updateMetaData(hashRing.getNodeByName(n.name), OPERATIONS.START);

            } catch (IOException e) {
                logger.error("[ECS] Error running cmd: " + e);
                e.printStackTrace();
            }
        }

        pushHashRingInTree();

        return true;
    }

    @Override
    public boolean stop() throws Exception {

        Collection<ECSNode> toStop = new ArrayList<>();

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();
            toStop.add(n);
        }

        for (ECSNode n : toStop) {

            logger.info("[ECS] Setting " + n.getNodeName() + " to STOPPED state!");

            hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.STOPPED);

            updateMetaData(hashRing.getNodeByName(n.name), OPERATIONS.STOP);
        }

        return pushHashRingInTree();
    }

    @Override
    public boolean shutdown() {

        // for each active node, disconnect them? call KVStore??

        Collection<String> toRemove = new ArrayList<>();

        try {
            for (Map.Entry<String, ECSNode> entry : availableServers.entrySet()) {

                logger.info("[ECS] Setting " + entry.getValue().getNodeName() + " to STOPPED state!");
                entry.getValue().setServerStateType(KVMessage.ServerStateType.SHUT_DOWN);
            }

            for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
                ECSNode n = entry.getValue();

                hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.SHUT_DOWN);

                toRemove.add(n.name);

                removeNodes(toRemove);
            }

            boolean metadata_backed = pushHashRingInTree();  // TODO: Check

            ZKAPP.close();

            String cmd = PWD + "/" + ZK_STOP_CMD;

            try {
                logger.info("[ECS] shutting down zookeeper...");
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

            return metadata_backed;
            //return true;
        } catch (Exception e) {
            logger.error("[ECS] error shutting down... " + e);
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
        if (rNode.getServerStateType().equals(KVMessage.ServerStateType.STARTED)) {

            logger.error("[ECS] Trying to START a non STOPPED node: " + rNode.getNodeName());
            return null;
        } else {


            logger.info("[ECS] Adding node: " + rNode.getNodeName());

            assert rNode.getNodeHash() != null;

            hashRing.addNode(rNode);
        }

        return rNode;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {

        logger.info("[ECS] Initiating storage service...");
        INITIALIZED = true;

        if (availableServers.size() == 0) {
            logger.info("[ECS] No storage service available...");

            return null;
        }

        return setupNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {

        logger.info("[ECS] Initiating setting up nodes...");

        Collection<IECSNode> result = new ArrayList<>();

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

                byte[] metaData = node.getMetaData().getServerStateType().toString().getBytes();

                String nodePath = ZK_SERVER_PATH + "/" + node.getNodeName();

                if (zk.exists(nodePath, true) == null) {
                    logger.info("[ECS] creating znode for " + node.getNodeName());
                    ZKAPP.create(nodePath, metaData);

                    String znodePath = ZK_SERVER_PATH + "/" + node.getNodeName() + "/operation";
                    ZKAPP.create(znodePath, "INIT".getBytes());

                } else {
                    ZK.update(nodePath, metaData);
                    String znodePath = ZK_SERVER_PATH + "/" + node.getNodeName() + "/operation";

                    if (zk.exists(znodePath, true) == null) {
                        logger.info("[ECS] creating znode for " + node.getNodeName() + "/operation");

                        ZKAPP.create(znodePath, "INIT".getBytes());

                    } else {
                        ZK.update(znodePath, "INIT".getBytes());
                    }
                }
            }

        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
        assert result.size() != 0;

        pushHashRingInTree();

        try {
            start_script();
        } catch (Exception e) {
            logger.error("[ECS] cannot call start script! " + e);
            e.printStackTrace();
        }

        /*
        try {
            if (awaitNodes(count, ZK_TIMEOUT))
                return result;
            else {
                logger.error("[ECS] unknown error in awaitNode... ");
                return null;
            }


        } catch (Exception e) {
            logger.error("[ECS] error in awaitNode: " + e);
            e.printStackTrace();
            return null;
        }
         */

        return result;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();

            hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.IDLE);

            updateMetaData(hashRing.getNodeByName(n.name), OPERATIONS.INIT);
        }

        return ZKAPP.connectedSignal.await(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        logger.debug("[ECS]: Removing nodes: " + nodeNames);

        for (String name : nodeNames) {

            assert name != null;

            if (availableNodeKeys.contains(name)) {
                logger.error("[ECS] node is not in Hash Ring: " + name);
            } else {

                ECSNode node = availableServers.get(name);
                assert node != null;


                try {
                    updateMetaData(hashRing.getNodeByName(name), OPERATIONS.TRANSFER);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

                /*
                wait for transfer to finish?
                 */

                String znodePath = ZK_SERVER_PATH + "/" + name + "/operation";

                try {
                    if (Arrays.toString(ZK.read(znodePath)).equals("TRANSFER_FINISH")) {

                    }

                    try {
                        hashRing.getNodeByName(name).setServerStateType(KVMessage.ServerStateType.SHUT_DOWN);
                        String[] newHashRange = hashRing.removeNode(node);

                        logger.debug("[ECS] New hash range: " + newHashRange[0] + " : " + newHashRange[1]);

                        // Adding back to available servers
                        availableNodeKeys.add(name);

                        availableServers.get(name).setServerStateType(KVMessage.ServerStateType.SHUT_DOWN);

                        updateMetaData(node, OPERATIONS.SHUT_DOWN);
                    } catch (Exception e) {
                        logger.error("[ECS] Error removing nodes! " + e);
                        e.printStackTrace();
                        return false;
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
        return pushHashRingInTree();
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
/*
    public BigInteger mdHashServer(String host, int port) {
        return MD5.HashFromHostAddress(host, port);
    }
*/
    /**
     * Synch changes on hashring with ZK
     */
    /*

    metaData marshalling:

    serverStateType + DEL + startHash + DEL + endHash + DEL + OPERATION

     */

    /*
    public String getOperation(String msg) {

        String[] tokens = msg.split("\\" + Constants.DELIMITER);

        for (String token : tokens) {
            for (OPERATIONS o : OPERATIONS.values()) {
                if (token.equals(o.toString())) {
                    return token;
                }
            }
        }

        return null;
    }

     */

    private void updateMetaData(ECSNode n, OPERATIONS operation) throws KeeperException, InterruptedException {

        if (n == null) {
            logger.debug("[ECS]: null node for updating meta data");

        }

        assert n != null;
        String serverStateType = n.getServerStateType().toString();
        String startHash;
        String endHash;

        if (n.getNodeHashRange()[0] == null || n.getNodeHashRange()[0].equals(""))
            startHash = "";
        else
            startHash = n.getNodeHashRange()[0];

        if (n.getNodeHashRange()[1] == null || n.getNodeHashRange()[1].equals(""))
            endHash = "";
        else
            endHash = n.getNodeHashRange()[1];

        logger.info("[ECS] updating meta data for znode: " + n.name);
        logger.info("[ECS]     state: " + n.getServerStateType());
        logger.info("[ECS]     name: " + n.name);
        logger.info("[ECS]     cache: " + n.getCacheSize());
        logger.info("[ECS]     strat: " + n.getReplacementStrategy());
        logger.info("[ECS]     start hash: " + startHash);
        logger.info("[ECS]     end hash: " + endHash);

        assert n.name != null;
        assert n.getNodeHost() != null;
        assert n.getNodePort() != -1;
        assert n.getCacheSize() != -1;
        assert n.getReplacementStrategy() != null;

        String metaData = serverStateType + Constants.DELIMITER +
                n.name + Constants.DELIMITER +
                n.getNodeHost() + Constants.DELIMITER +
                n.getNodePort() + Constants.DELIMITER +
                n.getCacheSize() + Constants.DELIMITER +
                n.getReplacementStrategy() + Constants.DELIMITER +
                startHash + Constants.DELIMITER +
                endHash;

        byte[] data = metaData.getBytes();
        String znodePath = ZK_SERVER_PATH + "/" + n.name;

        if (zk.exists(znodePath, true) == null) {
            logger.warn("[ECS] znode: " + znodePath + " do not exist! Creating...");
            ZKAPP.create(znodePath, data);
        } else {
            ZK.update(znodePath, data);
        }

        if (operation == null || operation.toString().equals("")) {
            logger.warn("[ECS] null operation!");
        } else {

            String op = operation.toString();
            logger.info("[ECS] sending operation " + op + " to znode " + n.name);
            String opPath = znodePath + '/' + "operation";

            if (zk.exists(opPath, true) == null) {
                logger.warn("[ECS] znode: " + znodePath + " do not exist! Creating...");
                ZKAPP.create(opPath, op.getBytes());
            } else {
                ZK.update(opPath, op.getBytes());
            }
        }

    }

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
            logger.error("Interrupted");
            return false;
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }
    // TODO: restore mechanism


    public boolean ifAllValidServerNames(Collection<String> nodeNames) {

        for (String name : nodeNames) {
            assert name != null;

            if (availableNodeKeys.contains(name)) {
                logger.error("[ECS] node is not in Hash Ring: " + name);
            } else {

                ECSNode node = availableServers.get(name);
                if (node == null) {
                    return false;
                }
            }
        }
        return true;
    }

}
