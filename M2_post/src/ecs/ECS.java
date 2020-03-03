package ecs;

import app_kvECS.IECSClient;
import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.HashingFunction.MD5;
import shared.messages.KVMessage;

import java.io.*;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ECS implements IECSClient, Watcher {

    private Logger logger = Logger.getRootLogger();

    private static final String SERVER_JAR = "m2-server.jar";
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
    public static int ZK_PORT = 2181;
    private static final String ZK_ROOT_PATH = "/root";
    public static final String ZK_SERVER_PATH = "/server";
    public static final String ZK_HASH_TREE = "/metadata";

    private static final String PWD = System.getProperty("user.dir");
    //private static final String RUN_SERVER_SCRIPT = "script.sh";
    private static final String ZK_START_CMD = "zookeeper-3.4.11/bin/zkServer.sh start";
    private static final String ZK_STOP_CMD = "zookeeper-3.4.11/bin/zkServer.sh stop";
    private static final String WHITESPACE = " ";

    //public enum OPERATIONS {START, STOP, SHUT_DOWN, UPDATE, TRANSFER, INIT, STATE_CHANGE, KV_TRANSFER, TRANSFER_FINISH}


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

        // TODO: initial state of hash ring?
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
                "   > "+PWD+"/logs/Server_"+node.getNodePort()+".log &";
        try {
            Process p = Runtime.getRuntime().exec(sshCmd);
            Thread.sleep(100);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override

    public boolean start() {
        List<ECSNode> toStart = new ArrayList<>();
        CountDownLatch sig = new CountDownLatch(hashRing.getSize());

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();

            if (n.getServerStateType().equals(IKVServer.ServerStateType.STOPPED)) {
                toStart.add(n);
            }

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() +"/op";
            broadcast(msgPath, IECSNode.ECSNodeFlag.START.name(), sig);

            n.setFlag(ECSNodeMessage.ECSNodeFlag.START);
            n.setServerStateType(IKVServer.ServerStateType.STARTED);
        }

        pushHashRingInTree();
        return true;
    }


    @Override
    public boolean shutdown() throws Exception {
        // for each active node, disconnect them? call KVStore??

        CountDownLatch sig = new CountDownLatch(hashRing.getSize());

        boolean ret = true;
        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {

            logger.info("[ECS] Setting " + entry.getValue().getNodeName() + " to SHUTDOWN state!");
            ECSNode n = entry.getValue();
            n.setServerStateType(IKVServer.ServerStateType.SHUT_DOWN);

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() +"/op";
            broadcast(msgPath, IECSNode.ECSNodeFlag.SHUT_DOWN.name(), sig);

        }

        if (ret) {

            for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
                ECSNode n = entry.getValue();
                n.shutdown();
                availableServers.put(n.getNodeName(), n);
            }

            hashRing.removeAllNode();
            ret &=pushHashRingInTree();
        }

        return ret;
    }

    @Override
    public boolean stop() throws Exception {

        CountDownLatch sig = new CountDownLatch(hashRing.getSize());

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {

            logger.info("[ECS] Setting " + entry.getValue().getNodeName() + " to STOPPED state!");
            ECSNode n = entry.getValue();
            n.setServerStateType(IKVServer.ServerStateType.STOPPED);
            n.setFlag(ECSNodeMessage.ECSNodeFlag.SHUT_DOWN);

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() +"/op";
            broadcast(msgPath, IECSNode.ECSNodeFlag.STOP.name(), sig);

        }

        return pushHashRingInTree();
    }


    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize, boolean isSole) {
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
        hashRing.addNode(rNode);

        if(isSole){
            pushHashRingInTree();
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
                zk.create(ZK_SERVER_PATH, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        for (int i = 0; i < count; ++i) {
            System.out.println(i);
            ECSNode node = addNode(cacheStrategy, cacheSize, false);
            result.add(node);

            String nodePath = ZK_SERVER_PATH + "/" + node.getNodePort();

            Stat exists = null;
            try {
                exists = zk.exists(nodePath, false);
                if (exists == null) {

                    zk.create(nodePath, "".getBytes(), //TODO: metadata
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                } else {
                    zk.setData(nodePath, "".getBytes(), exists.getVersion());
                    // Delete all children (msg z-nodes)
                    List<String> children = zk.getChildren(nodePath, false);
                    for (String zn : children) {
                        String msgPath = nodePath + "/" + zn;
                        Stat ex = zk.exists(msgPath, false);
                        zk.delete(msgPath, ex.getVersion());
                    }
                }
            }
            catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        pushHashRingInTree();


       setupNodes(count, cacheStrategy, cacheSize);

       return null;

    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {

        CountDownLatch sig = new CountDownLatch(hashRing.getSize());
        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() +"/op";
            broadcast(msgPath, IECSNode.ECSNodeFlag.INIT.name(), sig);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            start_script(n);

        }
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {

        return false;
    }

    public void broadcast(String msgPath, String msg, CountDownLatch sig){
        try {
            zk.create(msgPath,msg.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            Stat exists = zk.exists(msgPath, this);
            if (exists == null) {
                sig.countDown();
                logger.debug("unable to create path "+ msgPath);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Exception when send ZK  msg at " + msgPath +
                    "\n" + e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()));
        }

    }



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


    @Override
    public void process(WatchedEvent watchedEvent) {

    }
}
