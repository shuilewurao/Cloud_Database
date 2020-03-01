package app_kvServer;

import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


import app_kvServer.CacheManager.CachePolicy;
import app_kvServer.Database.KVDatabase;

import client.KVStore;
import com.google.gson.Gson;
import ecs.ECSHashRing;
import ecs.ECSMetaData;
import ecs.ECSNode;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import shared.communication.ClientConnection;
import shared.HashingFunction.MD5;

import app_kvServer.CacheManager.LRU;
import app_kvServer.CacheManager.FIFO;
import app_kvServer.CacheManager.LFU;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import static ecs.ECS.*;


public class KVServer implements IKVServer, Runnable, Watcher {

    private static Logger logger = Logger.getRootLogger();
    private static boolean isHashed= true; // flag to distinguish from M1

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;

    private ServerSocket serverSocket;
    private boolean running;

    private ArrayList<Thread> threadList;
    private Thread serverThread;

    private CachePolicy Cache;
    private KVDatabase DB;

    // ECS-related variables
    private ServerStateType serverState;
    private boolean writeLocked;

    private int zkPort;
    private String zkHostName;
    private String serverName;

    private String serverHashing;
    private String zkNodePath;

    private ZooKeeper zk;

    private String hashRingString;
    private ECSHashRing hashRing;


    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        threadList = new ArrayList<Thread>();
        serverThread = null;

        switch (strategy) {
            case "FIFO":
                Cache = new FIFO(cacheSize);
                break;
            case "LRU":
                Cache = new LRU(cacheSize);
                break;
            case "LFU":
                Cache = new LFU(cacheSize);
                break;
            default:
                this.strategy = CacheStrategy.None;
                logger.error("Invalid Cache Strategy!");
                // TODO: handling
                break;
        }
        this.DB = new KVDatabase(port);
    }

    public KVServer(String name, String zkHostname, int zkPort) {
        this.zkPort = zkPort;
        this.zkHostName = zkHostname;
        this.serverName = name;


        this.serverHashing = zkHostname +":"+ zkPort;
        this.zkNodePath = ZK_SERVER_PATH + "/" + name;

        threadList = new ArrayList<Thread>();
        serverThread = null;

        this.DB = new KVDatabase(port);
        this.writeLocked = false;

        this.serverState = ServerStateType.STOPPED;
        this.writeLocked = false;

        // connect to ZK
        subscribeZooKeeper();
        // create the server's ZK node
        createZKNode();

        getMetaDataTreeFromZK();
        initKVServer(hashRing, cacheSize, strategy.name());

        // Start the server in stopped state
        this.run();
    }


    @Override
    public int getPort() {
        return this.serverSocket.getLocalPort();
    }

    @Override
    public String getHostname() {
        if (serverSocket != null)
            return serverSocket.getInetAddress().getHostName();
        else
            return null;
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return this.strategy;

    }

    @Override
    public int getCacheSize() {
        return this.cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        try{
            return DB.inStorage(key);
        }catch(Exception e){
            logger.debug("Unable to access data file on disk!");
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean inCache(String key) {
        if (strategy == CacheStrategy.None) return false;
        if (Cache == null) return false;
        return Cache.inCache(key);

    }

    @Override
    public String getKV(String key) throws Exception {
        try {
            String result = null;

            //TODO: test ONLY
            /*
            if (getCacheStrategy() != CacheStrategy.None && Cache != null) {
                result = Cache.getKV(key);
                if (result != null) {
                    logger.info("KV (GET) in CACHE:  " + key + " => " + result);
                    return result;
                }
            }
             */

            // not in Cache, then retrieve from DB
            String value = DB.getKV(key); // TODO: get this from DB
            if (getCacheStrategy() != CacheStrategy.None && value != null) {
                Cache.putKV(key, value);
                logger.info("KV (GET) in STORAGE: " + key + " => " + value);
            } else {
                // TODO
                //logger.error("KV (GET) is not found by key:" + key);
            }
            return value;

        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        try {

            KVMessage.StatusType status = DB.putKV(key, value);
            if (getCacheStrategy() != CacheStrategy.None) {
                if (Cache != null) {
                    Cache.putKV(key, value);
                    logger.info("KeyValue " + "[" + key + ": " + value + "]" +
                            " has been stored in cache.");
                } else {
                    logger.error("Cache does not exist.");
                }
            }
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }

    @Override
    public void clearCache() {
        if (Cache == null) {
            logger.error("Cache does not exist.");
        }
        Cache.clearCache();
    }

    @Override
    public void clearStorage() {
        // TODO Auto-generated method stub
        clearCache();
        logger.info("Clear Storage.");
        DB.clearStorage();
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection =
                            new ClientConnection(this, client);
                    Thread t = new Thread(connection);
                    t.start();
                    threadList.add(t);

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("Server stopped.");

    }

    private boolean isRunning() {
        return this.running;
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    @Override
    public void kill() {
        running = false;
        try {
            if(serverSocket != null){
                serverSocket.close();
            }
            // TODO: need to check with ECS
            /*
            if(isHashed){
                String curr_nodePath = ZK_HOST + "/" + this.serverName;
                try{
                    if (zk.exists(curr_nodePath, false) != null) {
                        zk.delete(curr_nodePath, zk.exists(curr_nodePath, false).getVersion());
                        logger.info( "Remove exist alive node");
                    } else {
                        logger.error(curr_nodePath + " NOT exist!");
                    }
                    zk.close();

                }catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                    logger.error("can not remove current node path");
                }
            }*/
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        running = false;
        for (int i = 0; i < threadList.size(); i++) {
            threadList.get(i).interrupt();
        }
        if (serverThread != null)
            serverThread.interrupt();
        kill();
        clearCache(); // TODO: or clear storage?
    }


    /**
     * ECS-related initialization
     */
    public void initKVServer(ECSHashRing metadata, int cacheSize, String strategy){

        switch (strategy) {
            case "FIFO":
                Cache = new FIFO(cacheSize);
                break;
            case "LRU":
                Cache = new LRU(cacheSize);
                break;
            case "LFU":
                Cache = new LFU(cacheSize);
                break;
            default:
                this.strategy = CacheStrategy.None;
                logger.error("Invalid Cache Strategy!");
                break;
        }

    }

    /**
     *  ECS-related start, start processing all client requests and all ECS requests
     */
    public void start(){
        // TODO: move run() & main() to this function
        serverState = ServerStateType.STARTED;
    }

    /**
     * ECS-related stop, reject all client requests and only process ECS requests
     */
    public void stop(){
        serverState = ServerStateType.STOPPED;
        List<String> children;
        try{
            children = zk.getChildren(zkNodePath, false, null);
            if (children.isEmpty()) {
                // re-register the watch
                zk.getChildren(zkNodePath, this, null);
                return;
            }
            assert children.size() == 1;
            String path = zkNodePath + "/" + children.get(0);
            zk.delete(path, zk.exists(path, false).getVersion());
            logger.info("Server shutdown");

            // if still running, register the watch again
            if (this.isRunning())
                zk.getChildren(zkNodePath, this, null);

        }catch (KeeperException | InterruptedException e) {
            logger.debug("Unable to process the watcher event");
            e.printStackTrace();
        }
    }

    /**
     *  ECS-related shutdown, exit the KVServer application
     */
    public void shutdown(){
        // TODO: clear storage
        serverState = ServerStateType.SHUT_DOWN;
        List<String> children;
        try{
            children = zk.getChildren(zkNodePath, false, null);
            if (children.isEmpty()) {
                // re-register the watch
                zk.getChildren(zkNodePath, this, null);
                return;
            }
            assert children.size() == 1;
            String path = zkNodePath + "/" + children.get(0);
            zk.delete(path, zk.exists(path, false).getVersion());
            close();
            logger.info("Server shutdown");

        }catch (KeeperException | InterruptedException e) {
            logger.debug("Unable to process the watcher event");
            e.printStackTrace();
        }
    }

    /**
     * ECS-related lock, for write operations
     */
    public void lockWrite(){
        logger.info("--Lock Write");
        this.writeLocked = true;
    }

    /**
     * TODO
     * ECS-related unlock, for write operations
     */
    public void unLockWrite(){
        logger.info("--Unlock Write");
        this.writeLocked = false;
    }

    /**
     * TODO: range check
     * ECS-related moveData, move the given hashRange to the server going by the targetName
     * @param range
     * @param server
     * @return true if data transfer is completed
     */
    public boolean moveData(String[] range, String server) {

        if(range.length < 2){
            logger.debug("Invalid hash range for move data");
            return false;
        }
        // TODO: change meta data?
       this.lockWrite();

        BigInteger hashTarget = MD5.HashInBI(server);
        ECSNode targetServerNode = this.hashRing.getActiveNodes().get(hashTarget);
        ECSMetaData targetServer;
        if(targetServerNode != null){
            targetServer = targetServerNode.getMetaData();
        }
        else{
            logger.error("Could not find the target server for moving data");
            return false;
        }


        int port = targetServer.getPort();
        String address = targetServer.getHost();
        logger.info("Find the target server as ("+address+":"+port+")");

        //return byte array of Data
        try{
            String DataResult = DB.getPreMovedData(range);
            KVStore tempClient = new KVStore(address, port);
            tempClient.connect();

            // TODO: Needs a message here and check its status
            TextMessage result = tempClient.sendMovedData(DataResult);
            tempClient.disconnect();

            if(result.getMsg().equals("Transferring_Data_SUCCESS")){
                DB.deleteKVPairByRange(range);
                this.unLockWrite();
                return true;
            }

            this.unLockWrite();
            return false;


        }catch(Exception e){
            logger.error("Exceptions in getting moved data");

        }finally{
            // if not returned yet, it is a failure
            this.unLockWrite();
            return false;

        }

    }



    /**
     *
     * @return server's current state
     */
    public ServerStateType getServerState(){
        return serverState;
    }

    /**
     *
     * @return whether it is locked for write
     */
    public boolean isWriteLocked(){
        return writeLocked;
    }

    /**
     * TODO
     * @return
     */
    public ECSHashRing getMetaData(){
        return hashRing;
    }

    /**
     * should
     */
    private void subscribeZooKeeper(){
        try {
            // need to connect to ZK before running
            final CountDownLatch connected_signal = new CountDownLatch(1);

            zk = new ZooKeeper(this.serverHashing, 300000000, new Watcher() {
                @Override
                public void process(WatchedEvent we) {
                    if (we.getState() == Event.KeeperState.SyncConnected) {
                        connected_signal.countDown();
                    }
                }
            });
            connected_signal.await();
        } catch (IOException ioe) {
            logger.debug("Unable to connect to zookeeper");
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }



    private void getMetaDataTreeFromZK(){
        try {
            // setup hashRing info
            byte[] hashRingData = zk.getData(ZK_HASH_TREE, new Watcher() {
                // handle hashRing update
                public void process(WatchedEvent we) {
                    try {
                        byte[] hashRingData = zk.getData(ZK_HASH_TREE, this, null);
                        hashRingString = new String(hashRingData);
                        hashRing = new ECSHashRing(hashRingString);

                        logger.info("Hash Ring updated");

                    } catch (KeeperException | InterruptedException e) {
                        logger.error("Unable to update the metadata node");
                        e.printStackTrace();
                    }
                }
            }, null);

            // TODO
            hashRingString = new String(hashRingData);
            hashRing = new ECSHashRing(hashRingString);


        } catch (InterruptedException | KeeperException e) {
            logger.debug("Unable to get metadata info");
            e.printStackTrace();
        }

    }


    private void createZKNode(){
        try {
            // the node should be created before init the server
            if (zk.exists(zkNodePath, false) != null) {
                // retrieve cache info from zookeeper
                byte[] cacheData = zk.getData(zkNodePath, false, null);
                String cacheString = new String(cacheData);

                // TODO: if up-to-date
                ECSMetaData metaData = new Gson().fromJson(cacheString, ECSMetaData.class);
                this.cacheSize = metaData.getCacheSize();
                this.strategy = CacheStrategy.valueOf(metaData.getReplacementStrategy());
                logger.info("This server has been initialized as "+strategy + "with cache size:" + cacheSize);

            } else {
                logger.error("Server node dose not exist " + zkNodePath);
            }
        } catch (InterruptedException | KeeperException e) {
            logger.error("Unable to retrieve cache info from " + zkNodePath);
            // set up with some arbitrary default values,
            this.strategy = CacheStrategy.FIFO;
            this.cacheSize = 10;
            e.printStackTrace();
        }


//        try {
//            //TODO: remove the init message if have
//            List<String> children = zk.getChildren(zkNodePath, true);
//            if (!children.isEmpty()) {
//                String messagePath = zkNodePath + "/" + children.get(0);
//                byte[] data = zk.getData(messagePath, false, null);
//                KVAdminMessage message = new Gson().fromJson(new String(data), KVAdminMessage.class);
//                if (message.getOperationType().equals(KVAdminMessage.OperationType.INIT)) {
//                    zk.delete(messagePath, zk.exists(messagePath, false).getVersion());
//                    logger.info("Server "+serverName+ "already initialized with meta data");
//                }
//            }
//        } catch (InterruptedException | KeeperException e) {
//            logger.error("Unable to get child nodes");
//            e.printStackTrace();
//        }
    }

//    // TODO: check with ECS
//    private void createFailureDetectionNode(){
//        try {
//            // add an alive node for failure detection
//            if (zk.exists(ECS.ZK_ACTIVE_ROOT, false) != null) {
//                String alivePath = ECS.ZK_ACTIVE_ROOT + "/" + this.serverName;
//                zk.create(alivePath, "".getBytes(),
//                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//                logger.info(prompt() + "Alive node created");
//            } else {
//                logger.fatal(prompt() + "Active root not exist!");
//            }
//
//        } catch (KeeperException | InterruptedException e) {
//            logger.error(prompt() + "Unable to create an ephemeral node");
//            e.printStackTrace();
//        }
//    }

    @Override
    public void process(WatchedEvent event) {

    }




    /**
     * Update the forwarderList based on information in hashRing provided
     *
     * @param hashRing hashRing object
     * @throws IOException socket connection issue
     */
    public void update(ECSHashRing hashRing){
        ECSNode node = hashRing.getNodeByName(this.serverName);
        if (node == null) {
            // server idle or shutdown
            this.clearStorage();
            return;
        }

        //TODO

        /*

        List<KVServerForwarder> newList = hashRing.getReplicationNodes(node).stream()
                .map(KVServerForwarder::new).collect(Collectors.toList());

        // Can NOT replace with foreach since can NOT remove item
        // while iterating
        for (Iterator<KVServerForwarder> it = forwarderList.iterator();
             it.hasNext(); ) {
            KVServerForwarder forwarder = it.next();
            // Remove forwarder not longer active
            if (!newList.contains(forwarder)) {
                logger.info(self.getNodeName() + " disconnect from " + forwarder.getName());
                forwarder.disconnect();
                it.remove();
            }
        }

        for (KVServerForwarder forwarder : newList) {
            if (!this.forwarderList.contains(forwarder)) {
                logger.info(self.getNodeName() + " connects to " + forwarder.getName());
                forwarder.setPrompt(self.getNodeName() + " to " + forwarder.getName());
                forwarder.connect();
                this.forwarderList.add(forwarder);
            }
        }

         */
    }

    public String getServerName(){
        return this.serverName;
    }


    public String getHashRingStr(){
        return hashRingString;
    }

    public ECSHashRing getHashRing(){
        return hashRing;
    }

    public boolean isResponsible(String key){

        ECSNode node = hashRing.getNodeByHash(MD5.HashInBI(key));
        if (node == null) {
            logger.error("No node in hash ring is responsible for key " + key);
            return false;
        }
        Boolean responsible = node.getNodeName().equals(serverName);

        return responsible;
    }


    public static void main(String[] args) throws IOException {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                logger.error("Error! Invalid number of arguments!");
                logger.error("Usage: Server <port> <cacheSize> <strategy>!");
            } else {
                //String serverName = args[0];
                //String zkHostName = args[1];
                //int zkPort = Integer.parseInt(args[2]);
                int serverName = Integer.parseInt(args[0]);
                int zkHostName = Integer.parseInt(args[1]);
                String zkPort = args[2];
                KVServer server = new KVServer(
                        serverName,
                        zkHostName,
                        zkPort
                );
                new Thread(server).start();
            }
        } catch (IOException e) {
            logger.error("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) { //TODO
            logger.error("Error! Invalid argument format!");
            logger.error("Usage: Server <port> <cacheSize> <strategy>!");
            System.exit(1);
        }
    }
}
