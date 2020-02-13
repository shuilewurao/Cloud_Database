package app_kvServer;

import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;


import app_kvServer.CacheManager.CachePolicy;
import app_kvServer.DataObjects.MetaData;
import app_kvServer.Database.KVDatabase;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.communication.ClientConnection;

import app_kvServer.CacheManager.LRU;
import app_kvServer.CacheManager.FIFO;
import app_kvServer.CacheManager.LFU;
import shared.messages.KVMessage;


public class KVServer implements IKVServer, Runnable {

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

    // ECS-related variables
    private ServerStateType serverState;
    private boolean writeLocked;

    private int zkPort;
    private String zkHostName;
    private String serverName;

    // TODO
    private TreeMap<BigInteger, MetaData> metaDataTree;


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


        this.DB = new KVDatabase(port);
    }

    public KVServer(String name, String zkHostname, int zkPort) {
        this.zkPort = zkPort;
        this.zkHostName = zkHostname;
        this.serverName = name;
        threadList = new ArrayList<Thread>();
        serverThread = null;
//        String[] serverInfo = this.serverName.split(":");
//        this.port = Integer.parseInt(serverInfo[2]);
        this.DB = new KVDatabase(port);
        this.serverState = ServerStateType.STOPPED;
        this.writeLocked = false;
//        connectZooKeeper(zkHostName, zkPort);
//        getMetaDataFromZK();
//        initKVServer(metaData, cacheSize, strategy);
//        initialized = true;

        // start the server in stopped state
        // this.run();
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
            serverSocket.close();
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
     * ECS-related initialization
     * TODO: where to put
     */
    public void initKVServer(MetaData metadata, int cacheSize, String replacementStrategy){

        // TODO: metadata
        this.cacheSize = cacheSize;

        this.strategy = CacheStrategy.valueOf(replacementStrategy);

        switch (replacementStrategy) {
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

    }

    /**
     *  ECS-related start, start processing all client requests and all ECS requests
     */
    public void start(){
        serverState = ServerStateType.STARTED;
    }

    /**
     * ECS-related stop, reject all client requests and only process ECS requests
     */
    public void stop(){
        serverState = ServerStateType.STOPPED;
    }

    /**
     *  ECS-related shutdown, exit the KVServer application
     */
    public void shutdown(){
        serverState = ServerStateType.SHUT_DOWN;
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
     * TODO: range
     * ECS-related moveData, move the given hashRange to the server going by the targetName
     * @param range
     * @param server
     * @return true if data transfer is completed
     */
    public boolean moveData(String[] range, String server){
        return true;
    }

    /**
     * ECS-related update, update the metadata repo of this server
     * @param metadata
     * TODO
     */
    public void update(MetaData metadata){

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
    public TreeMap<BigInteger, MetaData> getMetaData(){
        return null;
    }


    public static void main(String[] args) throws IOException {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length != 3) {
                logger.error("Error! Invalid number of arguments!");
                logger.error("Usage: Server <port> <cacheSize> <strategy>!");
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
