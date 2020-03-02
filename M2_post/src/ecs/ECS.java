package ecs;

import app_kvECS.IECSClient;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import shared.HashingFunction.MD5;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

public class ECS implements IECSClient {

    private Logger logger = Logger.getRootLogger();

    private static final String SERVER_JAR = "m2-server.jar";
    // Assumes that the jar file is located at the same dir on the remote server
    private static final String JAR_PATH = new File(System.getProperty("user.dir"), SERVER_JAR).toString();

    private HashMap<String, ECSNode> availableServers;


    // Logical Hash Ring object managing the ECSNodes
    private ECSHashRing hashRing;
    //private List<String> availableNodeKeys;

    private boolean INITIALIZED = false;

    /*
    ZooKeeper instance
     */
    private static ZK ZKAPP = new ZK();
    private ZooKeeper zk;
    public static final String ZK_HOST = "127.0.0.1";
    public static int ZK_PORT = 2181;
    private static final String ZK_ROOT_PATH = "/root";
    public static final String ZK_SERVER_PATH = "/server";
    public static final String ZK_HASH_TREE = "/metadata";

    private static final String PWD = System.getProperty("user.dir");
    //private static final String RUN_SERVER_SCRIPT = "script.sh";
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

        hashRing = new ECSHashRing();
        pushHashRingInTree();

        logger.info("[ECS] New Logical Hash Ring with size: " + hashRing.getSize());

    }

    public void start_script(ECSNode node){
        String javaCmd = String.join(" ",
                "java -jar",
                JAR_PATH,
                String.valueOf(node.getNodePort()),
                String.valueOf(node.getCacheSize()),
                node.getReplacementStrategy());
        String sshCmd = "ssh -o StrictHostKeyChecking=no -n " + "localhost" + " nohup " + javaCmd +
                " &";
        try {
            Process p = Runtime.getRuntime().exec(sshCmd);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override

    public boolean start() throws Exception {

        return true;
    }

    @Override
    public boolean stop() throws Exception {
        return false;
    }

    @Override
    public boolean shutdown() throws Exception {
        return false;
    }

//    @Override
//    public boolean stop() throws Exception {
//
//        Collection<ECSNode> toStop = new ArrayList<>();
//
//        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
//            ECSNode n = entry.getValue();
//            toStop.add(n);
//        }
//
//        for (ECSNode n : toStop) {
//
//            logger.info("[ECS] Setting " + n.getNodeName() + " to STOPPED state!");
//
//            hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.STOPPED);
//
//            updateMetaData(hashRing.getNodeByName(n.name), OPERATIONS.STOP);
//        }
//
//        return pushHashRingInTree();
//    }
//
//    @Override
//    public boolean shutdown() {
//
//        // for each active node, disconnect them? call KVStore??
//
//        Collection<String> toRemove = new ArrayList<>();
//
//        try {
//            for (Map.Entry<String, ECSNode> entry : availableServers.entrySet()) {
//
//                logger.info("[ECS] Setting " + entry.getValue().getNodeName() + " to STOPPED state!");
//                entry.getValue().setServerStateType(KVMessage.ServerStateType.SHUT_DOWN);
//            }
//
//            for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
//                ECSNode n = entry.getValue();
//
//                hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.SHUT_DOWN);
//
//                toRemove.add(n.name);
//
//                removeNodes(toRemove);
//            }
//
//            boolean metadata_backed = pushHashRingInTree();  // TODO: Check
//
//            ZKAPP.close();
//
//            String cmd = PWD + "/" + ZK_STOP_CMD;
//
//            try {
//                logger.info("[ECS] shutting down zookeeper...");
//                Process process = Runtime.getRuntime().exec(cmd);
//
//                StringBuilder output = new StringBuilder();
//
//                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//
//                String line;
//                while ((line = reader.readLine()) != null) {
//                    output.append(line).append("\n");
//                }
//
//                int exitVal = process.waitFor();
//                if (exitVal == 0) {
//                    logger.info("[ECS] cmd success: " + cmd);
//                    logger.info(output);
//                    System.out.println("Success!");
//                    System.out.println(output);
//                } else {
//                    logger.error("[ECS] cmd abnormal: " + cmd);
//                    logger.error("[ECS] ZooKeeper cannot stop!");
//                }
//            } catch (IOException | InterruptedException e) {
//                e.printStackTrace();
//                logger.error("[ECS] ZooKeeper cannot stop! " + e);
//                System.exit(1);
//            }
//
//            return metadata_backed;
//            //return true;
//        } catch (Exception e) {
//            logger.error("[ECS] error shutting down... " + e);
//            e.printStackTrace();
//        }
//
//        return false;
//    }

    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize) {
        if(availableServers.size() ==0){
            return null;
        }
        Map.Entry<String,ECSNode> entry =availableServers.entrySet().iterator().next();
        String key = entry.getKey();
        ECSNode rNode = entry.getValue();

        /*
        Randomly pick one of the idle servers in the repository
         */
        // Get a random node from available servers
        availableServers.remove(key);


        rNode.init(cacheSize, cacheStrategy);
        start_script(rNode);
        hashRing.addNode(rNode);


        return rNode;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {


        logger.info("[ECS] Initiating storage service...");
        INITIALIZED = true;

        Collection<IECSNode> result = new ArrayList<>();

        if (availableServers.size() == 0) {
            logger.info("[ECS] No storage service available...");

            return null;
        }


            for (int i = 0; i < count; ++i) {
                System.out.println(i);
                ECSNode node = addNode(cacheStrategy, cacheSize);
                result.add(node);

            }


        return setupNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {


        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        return false;
    }
//
//    @Override
//    public boolean awaitNodes(int count, int timeout) throws Exception {
//
//        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
//            ECSNode n = entry.getValue();
//
//            hashRing.getNodeByName(n.name).setServerStateType(KVMessage.ServerStateType.IDLE);
//
//            updateMetaData(hashRing.getNodeByName(n.name), OPERATIONS.INIT);
//        }
//
//        return ZKAPP.connectedSignal.await(timeout, TimeUnit.MILLISECONDS);
//    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
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
//
//    private void updateMetaData(ECSNode n, OPERATIONS operation) throws KeeperException, InterruptedException {
//
//        if (n == null) {
//            logger.debug("[ECS]: null node for updating meta data");
//
//        }
//
//        assert n != null;
//        String serverStateType = n.getServerStateType().toString();
//        String startHash;
//        String endHash;
//
//        if (n.getNodeHashRange()[0] == null || n.getNodeHashRange()[0].equals(""))
//            startHash = "";
//        else
//            startHash = n.getNodeHashRange()[0];
//
//        if (n.getNodeHashRange()[1] == null || n.getNodeHashRange()[1].equals(""))
//            endHash = "";
//        else
//            endHash = n.getNodeHashRange()[1];
//
//        logger.info("[ECS] updating meta data for znode: " + n.name);
//        logger.info("[ECS]     state: " + n.getServerStateType());
//        logger.info("[ECS]     name: " + n.name);
//        logger.info("[ECS]     cache: " + n.getCacheSize());
//        logger.info("[ECS]     strat: " + n.getReplacementStrategy());
//        logger.info("[ECS]     start hash: " + startHash);
//        logger.info("[ECS]     end hash: " + endHash);
//
//        assert n.name != null;
//        assert n.getNodeHost() != null;
//        assert n.getNodePort() != -1;
//        assert n.getCacheSize() != -1;
//        assert n.getReplacementStrategy() != null;
//
//        String metaData = serverStateType + Constants.DELIMITER +
//                n.name + Constants.DELIMITER +
//                n.getNodeHost() + Constants.DELIMITER +
//                n.getNodePort() + Constants.DELIMITER +
//                n.getCacheSize() + Constants.DELIMITER +
//                n.getReplacementStrategy() + Constants.DELIMITER +
//                startHash + Constants.DELIMITER +
//                endHash;
//
//        byte[] data = metaData.getBytes();
//        String znodePath = ZK_SERVER_PATH + "/" + n.name;
//
//        if (zk.exists(znodePath, true) == null) {
//            logger.warn("[ECS] znode: " + znodePath + " do not exist! Creating...");
//            ZKAPP.create(znodePath, data);
//        } else {
//            ZK.update(znodePath, data);
//        }
//
//        if (operation == null || operation.toString().equals("")) {
//            logger.warn("[ECS] null operation!");
//        } else {
//
//            String op = operation.toString();
//            logger.info("[ECS] sending operation " + op + " to znode " + n.name);
//            String opPath = znodePath + '/' + "operation";
//
//            if (zk.exists(opPath, true) == null) {
//                logger.warn("[ECS] znode: " + znodePath + " do not exist! Creating...");
//                ZKAPP.create(opPath, op.getBytes());
//            } else {
//                ZK.update(opPath, op.getBytes());
//            }
//        }
//
//    }

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
        return true;
    }



}
