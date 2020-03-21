package ecs;

import app_kvECS.IECSClient;
import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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

    private LinkedHashMap<String, ECSNode> availableServers;

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
    public static final String ZK_LIVE_SERVERS = "/failure_detection";
    public static final String ZK_INIT_NODES = "/awaitNodes";
    public static final String ZK_CRASHED_NODES = "/crashed";

    private static final String PWD = System.getProperty("user.dir");
    //private static final String RUN_SERVER_SCRIPT = "script.sh";
    private static final String ZK_START_CMD = "zookeeper-3.4.11/bin/zkServer.sh start";
    //private static final String ZK_STOP_CMD = "zookeeper-3.4.11/bin/zkServer.sh stop";

    private int serverCount = 0;


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
                //System.out.println("Success!");
                //System.out.println(output);
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

            availableServers = new LinkedHashMap<>();

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

                logger.debug("[ECS] creating new node...");

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
        logger.debug("[ECS] Starting new ZooKeeper...");

        zk = ZKAPP.connect();

        logger.info("[ECS] ZooKeeper started!" + ZK_HOST);


        try {
            Stat s = zk.exists(ZK_INIT_NODES, true);
            if (s == null) {
                zk.create(ZK_INIT_NODES, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }else{
                List<String> children = zk.getChildren(ZK_INIT_NODES, false, null);
                if (!children.isEmpty()) {
                    for (int i = 0; i < children.size(); i++) {
                        zk.delete(ZK_INIT_NODES+"/"+children.get(i),-1);
                    }
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        try {
            if (zk.exists(ZK_LIVE_SERVERS, false) == null) {
                ZKAPP.create(ZK_LIVE_SERVERS, "".getBytes());
            }else{
                List<String> children = zk.getChildren(ZK_LIVE_SERVERS, false, null);
                if (!children.isEmpty()) {
                    for (int i = 0; i < children.size(); i++) {
                        ZK.deleteNoWatch(ZK_LIVE_SERVERS+"/"+children.get(i));
                    }
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            logger.error("Could not create the znode for monitoring the liveness of servers");
        }


        try {
            if (zk.exists(ZK_CRASHED_NODES, false) == null) {
                ZKAPP.create(ZK_CRASHED_NODES, "".getBytes());
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            logger.error("Could not create the znode for monitoring the liveness of servers");
        }

        logger.info("[ECS] Initializing Logical Hash Ring...");

        // TODO: initial state of hash ring?
        hashRing = new ECSHashRing();

        pushHashRingInTree();
        //pushHashRingInZnode();

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
        boolean ret = true;

        for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
            ECSNode n = entry.getValue();

            // Only start STOPPED nodes
            if (n.getServerStateType() == IKVServer.ServerStateType.STOPPED) {
                logger.debug("[ECS] node to start: " + n.name);

                toStart.add(n);
            }
        }

        CountDownLatch sig = new CountDownLatch(toStart.size());

        for (ECSNode n : toStart) {
            logger.debug("[ECS] node to start: " + n.name);

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;

            broadcast(msgPath, IECSNode.ECSNodeFlag.START.name(), sig);

            n.setFlag(ECSNodeMessage.ECSNodeFlag.START);
            n.setServerStateType(IKVServer.ServerStateType.STARTED);

            logger.info("[ECS] Start the node: " + n.name);
        }

        sig.await();

        ret &= pushHashRingInTree();
        //ret &= pushHashRingInZnode();

        if(ret==false){
            logger.debug("[ECS] Unable to fully start");
        }

        return ret;
    }



    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeout) {
            List<String> list = zk.getChildren(ZK_INIT_NODES, true);
            if (list.size() - serverCount == count) {
                serverCount += count;
                // TODO: setupReplica();
                return true;
            }
        }
        logger.info("[ECS] Noeds have been initialized.");

        return false;
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

            zk.delete(ZK_INIT_NODES + "/" + n.getPort(), -1);
            serverCount--;

        }

        hashRing.removeAllNode();

        sig.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS );
        logger.info("[ECS] Shut down all the servers.");
        //pushHashRingInZnode();

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
        //pushHashRingInZnode();
        return pushHashRingInTree();
    }


    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize, boolean isSole) {

        Collection<ECSNode> nodes = addNodes(1, cacheStrategy, cacheSize);

//        try {
//            awaitNodes(1, 30000);
//        } catch (Exception e) {
//           logger.error("[ECS] "+e);
//        }

        ECSNode added = nodes.iterator().next();

        boolean ret = maintainAddInvariant(added);

        logger.info("[ECS] "+added.getNodeName() +" has been added to service");

        CountDownLatch sig = new CountDownLatch(1);


        String msgPath = ZK_SERVER_PATH + "/" + added.getNodePort() + ZK_OP_PATH;

        broadcast(msgPath, IECSNode.ECSNodeFlag.START.name(), sig);

        added.setFlag(ECSNodeMessage.ECSNodeFlag.START);
        added.setServerStateType(IKVServer.ServerStateType.STARTED);

        logger.info("[ECS] Start the node: " + added.name);

        try {
            sig.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        ret &= pushHashRingInTree();

        return added;
    }


    public ECSNode addNode(String cacheStrategy, int cacheSize) {

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

        return rNode;
    }

    @Override
    public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {


        logger.info("[ECS] Initiating storage service...");

        List<ECSNode> result = new ArrayList<>();

        if (availableServers.size() == 0) {
            logger.info("[ECS] No storage service available...");

            return result;
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
            ECSNode node = addNode(cacheStrategy, cacheSize);

            result.add(node);

            String znodePath = ZK_SERVER_PATH + "/" + node.getNodePort();

            try {

                if (zk.exists(znodePath, false) == null) {

                    ZKAPP.create(znodePath, "".getBytes());

                } else {
                    ZK.update(znodePath, "".getBytes());
                    List<String> children = zk.getChildren(znodePath, false, null);
                    if (!children.isEmpty()) {
                        for (int j = 0; j < children.size(); j++) {
                            ZK.deleteNoWatch(znodePath+"/"+children.get(j));
                        }
                    }

                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        pushHashRingInTree();
        //pushHashRingInZnode();

        setupNodes(result);

        try {
            awaitNodes(count, 30000);
        } catch (Exception e) {
            logger.error("[ECS] "+e);
        }

        return result;

    }

    @Override
    public Collection<IECSNode> setupNodes(Collection<ECSNode> nodes){

        CountDownLatch sig = new CountDownLatch(nodes.size());

        for (ECSNode n: nodes) {
            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;
            broadcast(msgPath, IECSNode.ECSNodeFlag.INIT.name(), sig);
            logger.info("[ECS] Initializing " + n.getNodeHash());

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error("[ECS] Error! " + e);
                e.printStackTrace();
            }
            start_script(n);
        }

        // create znode with watches for failure detection on each alive server
        for(ECSNode n: nodes){
            registerFailureDetectionZNode(n.getNodePort(), n.getNodeHash());
        }
        try {
            // TODO
            sig.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.debug("[ECS] timeout when initializing");
        }

        return null;
    }

    private void registerFailureDetectionZNode(int node_port, String hash){

        int i;
        for(i=0; i<5;i++) {

            try {
                Stat exists = zk.exists(ZK_LIVE_SERVERS + "/" + node_port, null);
                zk.getData(ZK_LIVE_SERVERS + "/" + node_port,
                        new Watcher() {
                            @Override
                            public void process(WatchedEvent we) {
                                switch (we.getType()) {
                                    case NodeDeleted:
                                        //logger.warn("[ECS] Server" + hash + " shutdown detected");
                                        ECSNode n =  hashRing.getNodeByHash(MD5.HashInBI(hash));
                                        if(n==null){
                                            break;
                                        }
                                        switch (n.getServerStateType()) {
                                            case STARTED:
                                            case STOPPED:
                                                handleNodeFailure(n);
                                                break;
                                            default:
                                                break;
                                        }
                                        break;
                                    default:
                                        logger.warn("[ECS]: Failure Detector on Server " + node_port + " is fired for event " + we.getType());
                                }
                            }
                        }, null);
                logger.info("[ECS] Liveness Watcher has been set on server " + hash);
                return;
            } catch (KeeperException | InterruptedException e) {
                //e.printStackTrace();
                try {
                    Thread.sleep(i * 500);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
        logger.error("[ECS] Unable to create znode watcher on port " + node_port + " 's failure detection");
    }


    private void handleNodeFailure(ECSNode n){

        maintainRemoveInvariant(n);

        logger.warn("[ECS] Server" + n.getNodeHash() + " just crashed");

        serverCount--;
        try {
            zk.delete(ZK_INIT_NODES + "/" + n.getPort(), -1);
        } catch (InterruptedException | KeeperException e) {
            logger.error("[ECS] "+e);
        }

        boolean ret = true;

        n.shutdown();
        hashRing.removeNode(n);
        availableServers.put(n.getNodeName(), n);

        //ret &=pushHashRingInZnode();
        ret &= pushHashRingInTree();

        if(ret ==false){
            logger.error("[ECS] Errors in updating hash ring for handling server failure.");
        }

        pushHashRingInTree(); // notify the other nodes this failure first

        try {
            String path = ZK_CRASHED_NODES+"/"+n.getPort();
            if (zk.exists(path, false) == null) {
                ZKAPP.create(path, "".getBytes());
            }
        } catch (KeeperException | InterruptedException e) {
            //e.printStackTrace();
            logger.error("[ECS] "+e);
        }

        logger.info("[ECS] Adding a new server to replace the crashed one.");
        addNode("FIFO", 1,true);

//        try {
//            start();
//        } catch (KeeperException | InterruptedException e) {
//            logger.error("[ECS] Could not start the server when restoring a node failure");
//        }


        pushHashRingInTree();

    }



    public void broadcast(String msgPath, String msg, CountDownLatch sig) {

        try {
            if (zk.exists(msgPath, this) == null) {
                ZKAPP.create(msgPath, msg.getBytes());
                logger.debug("[ECS] ZK created " + msg + " in " +msgPath );
            } else {
                logger.warn("[ECS] " + msgPath + " already exists... updating to: "+msg+" and deleting children...");
                ZK.update(msgPath, msg.getBytes());
            }

            //if (zk.exists(msgPath, this) == null) {
            sig.countDown();
            //logger.debug("[ECS] Broadcast to  path " + msgPath + "="+zk.exists(msgPath, this) == null?"null":"created" );
            //}
        } catch (KeeperException | InterruptedException e) {
            logger.error("[ECS] Exception sending ZK msg at " + msgPath + ": " + e);
            e.printStackTrace();
        }
    }


    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        logger.debug("[ECS]: Removing nodes: " + nodeNames);

        if (hashRing.getSize() - nodeNames.size() < 3) {
            logger.warn("[ECS] Caution! You will have fewer than 3 nodes remaining.");
        }

        List<ECSNode> toRemove = new ArrayList<>();
        for (String name : nodeNames) {
            assert name != null;

            ECSNode n = hashRing.getNodeByServerName(name);
            if (n == null) {
                logger.error("[ECS] node is not in Hash Ring: " + name);
                continue;
            }
            toRemove.add(n);

            maintainRemoveInvariant(n);
            hashRing.removeNode(n);
        }


        boolean ret = true;
        CountDownLatch sig = new CountDownLatch(toRemove.size());
        for (ECSNode n : toRemove) {
            // TODO: check transfer finish
            logger.debug("[ECS] removing: " + n.getNodeName());

            String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;
            broadcast(msgPath, IECSNode.ECSNodeFlag.SHUT_DOWN.name(), sig);

            n.shutdown();
            availableServers.put(n.getNodeName(), n);

            try {
                zk.delete(ZK_INIT_NODES + "/" + n.getPort(), -1);

            } catch (InterruptedException | KeeperException e) {
                logger.error("[ECS] " + e);
            }
            serverCount--;
            logger.debug("[ECS] " + n.getNodeName() +" has been removed.");
        }
        //pushHashRingInZnode();
        ret &= pushHashRingInTree();

        try {
            sig.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.error("[ECS] latch timeout "+e);
        }

        return ret;


        // if only one node on ring

        // TODO
//        if (hashRing.getSize() == 1) {
//            logger.info("[ECS]: Only one node in hash ring!");
//            assert nodeNames.size() == 1;
//            for (String name : nodeNames) {
//
//                assert name != null;
//
//                ECSNode n = hashRing.getNodeByServerName(name);
//
//                if (n == null) {
//                    logger.error("[ECS] node is not in Hash Ring: " + name);
//                    continue;
//                }
//
//                String msgPath = ZK_SERVER_PATH + "/" + n.getNodePort() + ZK_OP_PATH;
//                broadcast(msgPath, IECSNode.ECSNodeFlag.SHUT_DOWN.name(), sig);
//
//                n.shutdown();
//                hashRing.removeNode(n);
//                availableServers.put(n.getNodeName(), n);
//            }
//            return true;
//        }

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

//    private boolean pushHashRingInZnode() {
//
//        if (hashRing.getSize() == 0) {
//            logger.warn("[ECS] Empty hash ring!");
//            return true;
//        }
//
//        try {
//
//            if (zk.exists(ZK_SERVER_PATH, false) == null) {
//                ZKAPP.create(ZK_SERVER_PATH, "".getBytes());  // NOTE: has to be persistent
//            } else {
//
//
//                for (Map.Entry<BigInteger, ECSNode> entry : hashRing.getActiveNodes().entrySet()) {
//
//                    ECSNode n = entry.getValue();
//
//                    String zDataPath = ZK_SERVER_PATH + "/" + n.getNodePort() + "/metadata";
//
//                    if (zk.exists(zDataPath, false) == null) {
//                        ZKAPP.create(zDataPath, n.getNodeJson().getBytes());  // NOTE: has to be persistent
//                    } else {
//                        ZK.update(zDataPath, n.getNodeJson().getBytes());
//                    }
//                }
//
//            }
//        } catch (InterruptedException e) {
//            logger.error("[ECS] Interrupted! " + e);
//            e.printStackTrace();
//            return false;
//        } catch (KeeperException e) {
//            logger.error("[ECS] Error! " + e);
//            e.printStackTrace();
//            return false;
//        }
//        return true;
//    }

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

    /**
     *
     * @param added previously idle servers
     */
    private boolean maintainAddInvariant(ECSNode added){
        // clear storage for non-restored server;

        if(!hashRing.isReplicable()){
            return false;
        }
        List<ECSDataReplication> toReplicate = new ArrayList<>();

        boolean ret=true;
        if(hashRing.getSize() == 3){
            ECSNode cNode = hashRing.getPrevNode(added.getNodeHash());
            toReplicate.add(new ECSDataReplication(cNode, added, new String[]{added.getNodeHash(), added.getNodeHash()}));
        }else{
            logger.info("[ECS] Identify add invariant conditions");
            String[] responsibleRange = hashRing.getResponsibleHashRange(added);
            ECSNode next = hashRing.getNextNode(added.getNodeHash());
            toReplicate.add(new ECSDataReplication(next, added, responsibleRange));

            Collection<ECSNode> coordinators = this.hashRing.getPredecessors(added);
            //Collection<ECSNode> replicas = this.hashRing.getReplicas(added);

            for(ECSNode c: coordinators){
                ECSNode toClear =  hashRing.getOldLastReplication(c);
                toReplicate.add(new ECSDataReplication(toClear, c.getNodeHashRange()));
            }
        }

        try{
            for (ECSDataReplication replication : toReplicate) {
                ret &= replication.start(zk);
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            ret &= false;
        } catch (KeeperException e) {
            e.printStackTrace();
            ret &= false;
        }finally{
            return ret;
        }

    }


    /**
     * IMP: the removing node is still on the ring
     * @param remove
     */
    private boolean maintainRemoveInvariant(ECSNode remove){
        // clear storage for non-restored server;

        List<ECSDataReplication> toReplicate = new ArrayList<>();

        boolean ret=true;
        if(hashRing.getSize() == 3){
            // No data transfer is needed
        }else{
            logger.info("[ECS] Identify remove invariant conditions");
            Collection<ECSNode> coordinators = hashRing.getPredecessors(remove);
            for(ECSNode c: coordinators){
                ECSNode toReceive =  hashRing.getOldLastReplication(c);
                toReplicate.add(new ECSDataReplication(c, toReceive, c.getNodeHashRange()));
            }


            ECSNode nextNode = hashRing.getNextNode(remove.getNodeHash());
            toReplicate.add(new ECSDataReplication(
                    nextNode,
                    hashRing.getLastReplication(nextNode), // this replica also extends its replication range
                    remove.getNodeHashRange()));
        }

        try{
            for (ECSDataReplication replication : toReplicate) {
                ret &= replication.start(zk);
            }
        }
        catch (InterruptedException | KeeperException e) {
            logger.error("[ECS] maintain remove invariant "+e );
            ret &= false;
        } finally{
            return ret;
        }

    }



}