package app_kvServer;

import app_kvServer.CacheManager.CachePolicy;
import app_kvServer.CacheManager.FIFO;
import app_kvServer.CacheManager.LFU;
import app_kvServer.CacheManager.LRU;
import app_kvServer.Database.KVDatabase;
import client.KVStore;
import ecs.*;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import shared.Constants;
import shared.HashingFunction.MD5;
import shared.communication.ClientConnection;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.io.*;
import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static ecs.ECS.*;


public class KVServer implements IKVServer, Runnable, Watcher {

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;

    private ServerSocket serverSocket;
    private boolean running;

    private ArrayList<Thread> threadList;
    private Thread serverThread;

    private CachePolicy cache;
    private KVDatabase DB;

    // ECS-related variables
    private ServerStateType serverState;
    private boolean writeLocked;

    private String serverName;

    private String zkNodePath;

    private static ZK ZKAPP = new ZK();
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
                cache = new FIFO(cacheSize);
                break;
            case "LRU":
                cache = new LRU(cacheSize);
                break;
            case "LFU":
                cache = new LFU(cacheSize);
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

        this.serverName = name;

        this.zkNodePath = ZK_SERVER_PATH + "/" + name;

        threadList = new ArrayList<>();
        serverThread = null;

        this.DB = new KVDatabase(port);
        this.writeLocked = false;

        this.serverState = ServerStateType.STOPPED;
        this.writeLocked = false;

        try {
            zk = ZKAPP.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        if (cache == null) return false;
        return cache.inCache(key);

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
                cache.putKV(key, value);
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
                if (cache != null) {
                    cache.putKV(key, value);
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
        if (cache == null) {
            logger.error("Cache does not exist.");
        }
        cache.clearCache();
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
                    logger.error("[KVStore] Error! " +
                            "Unable to establish connection. \n", e);
                }
            }
        }
        logger.info("[KVStore] Server stopped.");

    }

    private boolean isRunning() {
        return this.running;
    }

    private boolean initializeServer() {
        logger.info("[KVStore] Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("[KVStore] Server listening on port: "
                    + serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
            logger.error("[KVStore] Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("[KVStore] Port " + port + " is already bound!");
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
            logger.error("[KVStore] Error! " +
                    "Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        running = false;
        for (Thread thread : threadList) {
            thread.interrupt();
        }
        if (serverThread != null)
            serverThread.interrupt();
        kill();
        clearCache(); // TODO: or clear storage?
    }

    /**
     * ECS-related initialization
     */
    public void initKVServer(ECSHashRing metadata, int cacheSize, String strategy) {

        switch (strategy) {
            case "FIFO":
                cache = new FIFO(cacheSize);
                break;
            case "LRU":
                cache = new LRU(cacheSize);
                break;
            case "LFU":
                cache = new LFU(cacheSize);
                break;
            default:
                this.strategy = CacheStrategy.None;
                logger.error("[KVStore] Invalid Cache Strategy!");
                break;
        }

    }

    /**
     * ECS-related start, start processing all client requests and all ECS requests
     */
    public void start() {
        // TODO: move run() & main() to this function
        serverState = ServerStateType.STARTED;
    }

    /**
     * ECS-related stop, reject all client requests and only process ECS requests
     */
    public void stop() {
        serverState = ServerStateType.STOPPED;
        List<String> children;
        try {
            children = zk.getChildren(zkNodePath, false, null);
            if (children.isEmpty()) {
                // re-register the watch
                zk.getChildren(zkNodePath, this, null);
                return;
            }
            assert children.size() == 1;
            String path = zkNodePath + "/" + children.get(0);
            zk.delete(path, zk.exists(path, false).getVersion());
            logger.info("[KVStore] Server shutdown");

            // if still running, register the watch again
            if (this.isRunning())
                zk.getChildren(zkNodePath, this, null);

        } catch (KeeperException | InterruptedException e) {
            logger.debug("[KVStore] Unable to process the watcher event");
            e.printStackTrace();
        }
    }

    /**
     * ECS-related shutdown, exit the KVServer application
     */
    public void shutdown() {
        // TODO: clear storage
        serverState = ServerStateType.SHUT_DOWN;
        List<String> children;
        try {
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
            logger.info("[KVStore] Server shutdown");

        } catch (KeeperException | InterruptedException e) {
            logger.debug("[KVStore] Unable to process the watcher event");
            e.printStackTrace();
        }
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

    public boolean moveData(String[] range, String server) {

        if (range.length < 2) {
            logger.debug("Invalid hash range for move data");
            return false;
        }

        this.lockWrite();

        BigInteger hashTarget = MD5.HashInBI(server);
        ECSNode targetServerNode = this.hashRing.getActiveNodes().get(hashTarget);
        ECSMetaData targetServer;
        if (targetServerNode != null) {
            targetServer = targetServerNode.getMetaData();
        } else {
            logger.error("Could not find the target server for moving data");
            return false;
        }


        int port = targetServer.getPort();
        String address = targetServer.getHost();
        logger.info("Find the target server as (" + address + ":" + port + ")");

        //return byte array of Data
        try {
            String DataResult = DB.getPreMovedData(range);
            KVStore tempClient = new KVStore(address, port);
            tempClient.connect();

            // TODO: Needs a message here and check its status
            TextMessage result = tempClient.sendMovedData(DataResult);
            tempClient.disconnect();

            if (result.getMsg().equals("Transferring_Data_SUCCESS")) {
                DB.deleteKVPairByRange(range);
                this.unlockWrite();
                return true;
            }

            this.unlockWrite();
            return false;


        } catch (Exception e) {
            logger.error("Exceptions in getting moved data");

        } finally {
            // if not returned yet, it is a failure
            this.unlockWrite();
            return false;

        }

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

    public ECSHashRing getMetaData() {
        return hashRing;
    }

    private void getMetaDataTreeFromZK() {
        logger.info("[KVServer] Getting meta data for: " + serverName);
        try {

            byte[] hashRingData = zk.getData(ZK_HASH_TREE, new Watcher() {
                // handle hashRing update
                public void process(WatchedEvent we) {
                    try {
                        byte[] hashRingData = ZK.read(ZK_HASH_TREE);

                        hashRingString = new String(hashRingData);
                        hashRing = new ECSHashRing(hashRingString);

                    } catch (KeeperException | InterruptedException e) {
                        logger.debug("[KVServer] Unable to access metadata info");
                        e.printStackTrace();
                    }
                }
            }, null);

            hashRingString = new String(hashRingData);
            hashRing = new ECSHashRing(hashRingString);

        } catch (InterruptedException | KeeperException e) {
            logger.debug("[KVServer] Unable to get metadata info: " + e);
            e.printStackTrace();
        }

    }

    private void createZKNode() {
        logger.info("[KVServer] reading zk info for: " + serverName);
        try {

            if (zk.exists(zkNodePath, false) != null) {

                String data = new String(ZK.read(zkNodePath));
                String[] tokens = data.split("\\" + Constants.DELIMITER);

                if (tokens.length > 1) {
                    String name = tokens[1];
                    /*
                    String host = tokens[2];
                    int port = Integer.parseInt(tokens[3]);
                    int cacheSize = Integer.parseInt(tokens[4]);
                    String cacheStrategy = tokens[5];
                     */

                    assert serverName.equals(name);

                } else {
                    logger.warn("[KVServer] Not enough server info in zk for: " + serverName);
                }

            } else {
                logger.error("[KVServer] Server node does not exist: " + zkNodePath);
            }
        } catch (InterruptedException | KeeperException e) {
            logger.error("[KVServer] Unable to retrieve cache info from " + zkNodePath);
            // set up with some arbitrary default values,
            this.strategy = CacheStrategy.FIFO;
            this.cacheSize = 10;
            e.printStackTrace();
        }

        logger.info("[KVServer] reading OPERATION for: " + serverName);
        try {

            List<String> children = zk.getChildren(zkNodePath, false);

            for (String child : children) {
                String operation = new String(ZK.read(zkNodePath + "/" + child));

                if (operation.equals(OPERATIONS.INIT.name())) {
                    ZK.delete(zkNodePath + "/" + child);
                }

            }
        } catch (InterruptedException | KeeperException e) {
            logger.error("[KVServer] Unable to get child nodes: " + e);
            e.printStackTrace();
        }
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

            // handle operation

            String msg = new String(ZK.read(path));

            String[] tokens = msg.split("\\" + Constants.DELIMITER);

            assert tokens.length >= 1;

            logger.info("[KVServer] op msg: " + msg);

            switch (OPERATIONS.valueOf(tokens[0])) {
                case INIT:
                    ZK.delete(path);
                    logger.info("[KVServer] Server started!");
                    break;
                case SHUT_DOWN:
                    ZK.delete(path);
                    close();
                    logger.info("[KVServer] Server shutdown!");
                    break;
                case UPDATE:
                    break;

                case START:
                    this.start();
                    ZK.delete(path);
                    break;

                case STOP:
                    this.stop();
                    ZK.delete(path);
                    break;
            }

            if (this.isRunning())
                zk.getChildren(zkNodePath, this, null);

        } catch (KeeperException | InterruptedException e) {
            logger.debug("[KVStore] Unable to process the watcher event");
            e.printStackTrace();
        }
    }

    public void update(ECSHashRing hashRing) {
        ECSNode node = hashRing.getNodeByName(this.serverName);
        if (node == null) {
            // server idle or shutdown
            this.clearStorage();
        }
    }

    public String getServerName() {
        return this.serverName;
    }


    public String getHashRingStr() {
        return hashRingString;
    }

    public ECSHashRing getHashRing() {
        return hashRing;
    }

    public boolean isResponsible(String key) {

        ECSNode node = hashRing.getNodeByHash(MD5.HashInBI(key));
        if (node == null) {
            logger.error("[KVStore] No node in hash ring is responsible for key " + key);
            return false;
        }

        return node.getNodeName().equals(serverName);
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);

            if (args.length != 3) {
                logger.error("[KVStore] Error! Invalid number of arguments!");
                logger.error("[KVStore] Usage: Server <server_name> <zkHostName> <zkPort>!");
            } else {

                String serverName = args[0];
                String zkHostName = args[1];
                int zkPort = Integer.parseInt(args[2]);

                KVServer server = new KVServer(
                        serverName,
                        zkHostName,
                        zkPort
                );
                new Thread(server).start();
            }
        } catch (IOException e) {
            logger.error("[KVStore] Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) { //TODO
            logger.error("[KVStore] Error! Invalid argument format!");
            logger.error("[KVStore] Usage: Server <serverName> <zkHost> <zkPort>!");
            System.exit(1);
        }
    }
}