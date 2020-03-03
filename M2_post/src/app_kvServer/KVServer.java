package app_kvServer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
import ecs.*;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.Constants;
import shared.HashingFunction.MD5;
import shared.communication.ClientConnection;

import app_kvServer.CacheManager.LRU;
import app_kvServer.CacheManager.FIFO;
import app_kvServer.CacheManager.LFU;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import static ecs.ECS.ZK_HASH_TREE;
import static ecs.ECS.ZK_SERVER_PATH;


public class KVServer implements IKVServer, Runnable, Watcher {

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;

    private ServerSocket serverSocket;
    private boolean running;

    private ArrayList<Thread> threadList;
    private Thread serverThread;

    private CachePolicy Cache;
    private KVDatabase DB;

    private ServerStateType serverState;
    private boolean writeLocked;

    private static ZK ZKAPP = new ZK();
    private static ZooKeeper zk;

    private String hashRingString;
    private ECSHashRing hashRing;
    private String zkNodePath;

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
        // TODO Auto-generated method stub
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

        this.zkNodePath = ZK_SERVER_PATH + "/" + port;
        this.DB = new KVDatabase(port);
        serverState = ServerStateType.STOPPED;
        this.writeLocked = true;

        initKVServer();
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
        // TODO Auto-generated method stub
        try {
            return DB.inStorage(key);
        } catch (Exception e) {
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

            if (getCacheStrategy() != CacheStrategy.None && Cache != null) {
                result = Cache.getKV(key);
                if (result != null) {
                    logger.info("KV (GET) in CACHE:  " + key + " => " + result);
                    return result;
                }
            }

            // not in Cache, then retrieve from DB
            String value = DB.getKV(key); // TODO: get this from DB
            if (getCacheStrategy() != CacheStrategy.None && value != null) {
                Cache.putKV(key, value);
                logger.info("KV (GET) in STORAGE: " + key + " => " + value);
            } else {
                logger.error("KV (GET) is not found by key:" + key);
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
        clearCache();
        logger.info("Clear Storage.");
        DB.clearStorage();
    }

    @Override
    public void run() {

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
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        running = false;
        try {
            for (int i = 0; i < threadList.size(); i++) {
                threadList.get(i).interrupt();
            }
            if (serverThread != null)
                serverThread.interrupt();
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    /**
     * ECS-related start, start processing all client requests and all ECS requests
     */
    public void start() {
        // TODO: move run() & main() to this function
        serverState = ServerStateType.STARTED;
        unlockWrite();
        logger.info("[KVServer] Port " + port +"has started" );
    }

    /**
     * @return server's current state
     */
    public ServerStateType getServerState() {
        return serverState;
    }

    /**
     * @return whether it is locked for write
     */
    public boolean isWriteLocked() {
        return writeLocked;
    }

    public static void main(String[] args) throws IOException {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                logger.error("Error! Invalid number of arguments!");
                logger.error("Usage: Server <port> <cacheSize> <strategy>!");
            } else {
                int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String strategy = args[2];
                KVServer server = new KVServer(
                        port,
                        cacheSize,
                        strategy
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

    public void initKVServer(){
        try {
            zk = ZKAPP.connect();

        } catch (IOException e) {
            logger.debug("Unable to connect to zookeeper");
            e.printStackTrace();
        }


        try {
            // the node should be created before init the server
            Stat stat = zk.exists(zkNodePath, false);

            if (stat == null) {
                // create a node if does not exist
                zk.create(zkNodePath, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                logger.info( "[KVServer] Can not find ZK serverNode path , creating one ");
            }



        } catch (InterruptedException | KeeperException e) {
            logger.debug("[KVServer] Unable to create ZK path " + zkNodePath);
            this.strategy = CacheStrategy.FIFO;
            this.cacheSize = 100;
            e.printStackTrace();
        }

        try {
            //remove the init message
            List<String> children = zk.getChildren(zkNodePath , false, null);

            if (!children.isEmpty()) {
                logger.debug("checking ZK Msg: " + children.toString());
                String msgPath = zkNodePath  + "/" + children.get(0);
                byte[] data = zk.getData(msgPath, false, null);
                if (data.toString().equals(IECSNode.ECSNodeFlag.INIT.name())) {
                    zk.delete(msgPath, zk.exists(msgPath, false).getVersion());
                    logger.info( "Server initiated at constructor");
                }
            }
        } catch (InterruptedException | KeeperException e) {
            logger.debug( "Unable to get child nodes");
            e.printStackTrace();
        }

        try {
            // setup hashRing info
            byte[] hashRingData = zk.getData(ZK_HASH_TREE, new Watcher() {
                // handle hashRing update
                public void process(WatchedEvent we) {
                    if (!running){
                        return;
                    }
                    try {
                        byte[] hashRingData = zk.getData(ECS.ZK_HASH_TREE, this, null);
                        hashRingString = new String(hashRingData);
                        hashRing = new ECSHashRing(hashRingString);
                        logger.info("Hash Ring updated");
                    } catch (KeeperException | InterruptedException e) {
                        logger.info("Unable to update the metadata node");
                        e.printStackTrace();
                    }
                }
            }, null);
            logger.debug("Hash Ring found");
            hashRingString = new String(hashRingData);
            hashRing = new ECSHashRing(hashRingString);


        } catch (InterruptedException | KeeperException e) {
            logger.debug("Unable to get metadata info");
            e.printStackTrace();
        }


        try {
            // set watcher on children
            zk.getChildren(this.zkNodePath, this, null);
            logger.debug("Set up watcher on " +zkNodePath );
        } catch (InterruptedException | KeeperException e) {
            logger.error("Unable to get set watcher on children");
            e.printStackTrace();
        }

    }


    /**
     * ECS-related stop, reject all client requests and only process ECS requests
     */
    public void stop() {
        serverState = ServerStateType.STOPPED;
        lockWrite();
    }

    /**
     * ECS-related shutdown, exit the KVServer application
     */
    public void shutdown() {
        // TODO: clear storage
        lockWrite();
        serverState = ServerStateType.SHUT_DOWN;
        clearStorage();
        close();
        logger.info("[KVStore] Server shutdown");
    }

    /**
     * ECS-related lock, for write operations
     */
    public void lockWrite() {
        logger.info("[KVStore] --Lock Write");
        this.writeLocked = true;
    }

    /**
     * TODO
     * ECS-related unlock, for write operations
     */
    public void unlockWrite() {
        logger.info("[KVStore] --Unlock Write");
        this.writeLocked = false;
    }

//
//    public boolean moveData(String[] range, String server) {
//
//        if (range.length < 2) {
//            logger.debug("Invalid hash range for move data");
//            return false;
//        }
//
//        this.lockWrite();
//
//        BigInteger hashTarget = MD5.HashInBI(server);
//        ECSNode targetServerNode = this.hashRing.getActiveNodes().get(hashTarget);
//        ECSMetaData targetServer;
//        if (targetServerNode != null) {
//            targetServer = targetServerNode.getMetaData();
//        } else {
//            logger.error("Could not find the target server for moving data");
//            return false;
//        }
//
//
//        int port = targetServer.getPort();
//        String address = targetServer.getHost();
//        logger.info("Find the target server as (" + address + ":" + port + ")");
//
//        //return byte array of Data
//        try {
//            String DataResult = DB.getPreMovedData(range);
//            KVStore tempClient = new KVStore(address, port);
//            tempClient.connect();
//
//            // TODO: Needs a message here and check its status
//            TextMessage result = tempClient.sendMovedData(DataResult);
//            tempClient.disconnect();
//
//            if (result.getMsg().equals("Transferring_Data_SUCCESS")) {
//                DB.deleteKVPairByRange(range);
//                this.unlockWrite();
//                return true;
//            }
//
//            this.unlockWrite();
//            return false;
//
//
//        } catch (Exception e) {
//            logger.error("Exceptions in getting moved data");
//
//        } finally {
//            // if not returned yet, it is a failure
//            this.unlockWrite();
//            return false;
//
//        }
//
//    }
//

    public ECSHashRing getMetaData() {
        return hashRing;
    }

    @Override
    public void process(WatchedEvent event) {
        logger.info("[KVServer] watcher is triggered");
        List<String> children;

        try {
            children = zk.getChildren(zkNodePath, false);

            if (children.isEmpty()) {
                zk.getChildren(zkNodePath, this, null);
                return;
            }

            assert children.size() == 1;

            String path = zkNodePath + "/" + children.get(0);

            // handle node flag change
            String msg = new String(ZK.read(path));

            String[] tokens = msg.split("\\" + Constants.DELIMITER);

            assert tokens.length >= 1;

            logger.info("[KVServer] op msg: " + msg);

            switch (IECSNode.ECSNodeFlag.valueOf(tokens[0])) {
                case INIT:
                    ZK.deleteNoWatch(path);
                    logger.info("[KVServer] Server started!");
                    break;
                case SHUT_DOWN:
                    ZK.deleteNoWatch(path);
                    logger.info("[KVServer] Server shutdown!");
                    shutdown();

                    break;
//                case UPDATE:
//                    break;
//
                case START:
                    this.start();
                    ZK.deleteNoWatch(path);
                    break;
//
                case STOP:
                    this.stop();
                    ZK.deleteNoWatch(path);
                    logger.info("[KVServer] Server stopped!");
                    break;
                default:
                    logger.debug("[KVServer] process "+tokens[0]);
            }

            if (this.isRunning())
                zk.getChildren(zkNodePath, this, null);

        } catch (KeeperException | InterruptedException e) {
            logger.debug("[KVStore] Unable to process the watcher event");
            e.printStackTrace();
        }
    }
//
//    public void update(ECSHashRing hashRing) {
//        ECSNode node = hashRing.getNodeByName(this.serverName);
//        if (node == null) {
//            // server idle or shutdown
//            this.clearStorage();
//        }
//    }
//
//
//
    public String getHashRingStr() {
        return hashRingString;
    }

//    public ECSHashRing getHashRing() {
//        return hashRing;
//    }
//
    public boolean isResponsible(String key) {

        ECSNode node = hashRing.getNodeByHash(MD5.HashInBI(key));
        if (node == null) {
            logger.error("[KVStore] No node in hash ring is responsible for key " + key);
            return false;
        }

        return node.getNodePort()==port;
    }

}
