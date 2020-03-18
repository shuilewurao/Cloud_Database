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
import org.apache.zookeeper.data.Stat;

import org.apache.zookeeper.*;

import shared.Constants;
import shared.HashingFunction.MD5;
import shared.communication.ClientConnection;
import shared.messages.KVMessage;
import shared.messages.TextMessage;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static ecs.ECS.ZK_HASH_TREE;
import static ecs.ECS.ZK_SERVER_PATH;

public class KVServer implements IKVServer, Runnable, Watcher {

    private static Logger logger = Logger.getRootLogger();

    private String name;
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

    private KVServerDataReplicationManager dataReplicationManager;

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

        switch (port) {
            case 50000:
                this.name = "server1";
                break;
            case 50001:
                this.name = "server2";
                break;
            case 50002:
                this.name = "server3";
                break;
            case 50003:
                this.name = "server4";
                break;
            case 50004:
                this.name = "server5";
                break;
            case 50005:
                this.name = "server6";
                break;
            case 50006:
                this.name = "server7";
                break;
            case 50007:
                this.name = "server8";
                break;
            default:
                logger.error("[KVServer] Unknown port: " + port);
                close();
        }

        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        threadList = new ArrayList<>();
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
                logger.error("[KVServer] Invalid Cache Strategy!");
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
            logger.debug("[KVServer] Unable to access data file on disk!");
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
            String result;

            if (getCacheStrategy() != CacheStrategy.None && Cache != null) {
                result = Cache.getKV(key);
                if (result != null) {
                    logger.info("[KVServer] KV (GET) in CACHE:  " + key + " => " + result);
                    return result;
                }
            }

            // not in Cache, then retrieve from DB
            String value = DB.getKV(key); // TODO: get this from DB
            if (getCacheStrategy() != CacheStrategy.None && value != null) {
                Cache.putKV(key, value);
                logger.info("[KVServer] KV (GET) in STORAGE: " + key + " => " + value);
            } else {
                logger.error("[KVServer] KV (GET) is not found by key:" + key);
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
                    logger.info("[KVServer] KeyValue " + "[" + key + ": " + value + "]" +
                            " has been stored in cache.");
                } else {
                    logger.error("[KVServer] Cache does not exist.");
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
            logger.error("[KVServer] Cache does not exist.");
        }
        Cache.clearCache();
    }

    @Override
    public void clearStorage() {
        clearCache();
        logger.info("[KVServer] Clear Storage.");
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

                    logger.info("[KVServer] Connected to "
                            + client.getInetAddress().getHostName()
                            + " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("[KVServer] Error! " +
                            "Unable to establish connection. \n", e);
                    e.printStackTrace();
                }
            }
        }
        logger.info("[KVServer] Server stopped.");

    }

    private boolean isRunning() {
        return this.running;
    }

    private boolean initializeServer() {
        logger.info("[KVServer] Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("[KVServer] Server listening on port: "
                    + serverSocket.getLocalPort());

            this.dataReplicationManager = new KVServerDataReplicationManager(this.name, getHostname(), this.port);

            return true;

        } catch (IOException e) {
            logger.error("[KVServer] Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("[KVServer] Port " + port + " is already bound!");
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
            if (dataReplicationManager != null)
                dataReplicationManager.clear();

        } catch (IOException e) {
            logger.error("[KVServer] Error! " +
                    "Unable to close socket on port: " + port, e);
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        running = false;

        try {
            for (Thread thread : threadList) {
                thread.interrupt();
            }
            if (serverThread != null)
                serverThread.interrupt();
            serverSocket.close();
            ZKAPP.close();
        } catch (IOException e) {
            logger.error("[KVServer] Error! " +
                    "Unable to close socket on port: " + port, e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.error("Unable to close ZK session. ");
        }

    }

    /**
     * ECS-related start, start processing all client requests and all ECS requests
     */
    public void start() {
        // TODO: move run() & main() to this function
        serverState = ServerStateType.STARTED;
        unlockWrite();
        logger.info("[KVServer] Port " + port + " has started");
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
                logger.error("[KVServer] Error! Invalid number of arguments!");
                logger.error("[KVServer] Usage: Server <port> <cacheSize> <strategy>!");
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
            logger.error("[KVServer] Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) { //TODO
            logger.error("[KVServer] Error! Invalid argument format!");
            logger.error("[KVServer] Usage: Server <port> <cacheSize> <strategy>!");
            System.exit(1);
        }
    }

    public void initKVServer() {
        try {
            zk = ZKAPP.connect();

        } catch (IOException e) {
            logger.debug("[KVServer] Unable to connect to zookeeper! " + e);
            e.printStackTrace();
        }

        try {
            // the node should be created before init the server

            if (zk.exists(zkNodePath, false) == null) {
                // create a node if does not exist
                ZKAPP.create(zkNodePath, "".getBytes());
                logger.info("[KVServer] Can not find ZK serverNode path, creating one");
            }

        } catch (InterruptedException | KeeperException e) {
            logger.debug("[KVServer] Unable to create ZK path " + zkNodePath);
            this.strategy = CacheStrategy.FIFO;
            this.cacheSize = 100;
            e.printStackTrace();
        }


        try {
            //remove the init message
            List<String> children = zk.getChildren(zkNodePath, false, null);

            if (!children.isEmpty()) {

                String msgPath = zkNodePath + "/" + children.get(0);
                byte[] data = ZK.readNullStat(msgPath);
                logger.debug("[KVServer] checking ZK Msg: " + children.toString() + ": " + new String(data));
                if (new String(data).equals(IECSNode.ECSNodeFlag.INIT.name())) {
                    ZK.deleteNoWatch(msgPath);
                    logger.info("[KVServer] Server initiated at constructor");
                }
            }
        } catch (InterruptedException | KeeperException e) {
            logger.debug("[KVServer] Unable to get child nodes");
            e.printStackTrace();
        }


        try {
            // add an alive node for failure detection
            if (zk.exists(ECS.ZK_LIVE_SERVERS, false) != null) {
                String alivePath = ECS.ZK_LIVE_SERVERS + "/" + this.port;

                // This znode shall be deleted when the session ends (even unexpectedly).
                zk.create(alivePath, "".getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                logger.info("[KVServer] ZK node for failure detection created");
            } else {
                logger.fatal("[KVServer] ZK_LIVE_SERVERS root DNE!");
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Unable to create zknode for failure detection");
            e.printStackTrace();
        }


        try {
            // setup hashRing info
            // handle hashRing update
            byte[] hashRingData = zk.getData(ZK_HASH_TREE, new Watcher() {
                // handle hashRing update
                public void process(WatchedEvent we) {
                    if (!running) {
                        return;
                    }
                    try {
                        byte[] hashRingData1 = zk.getData(ECS.ZK_HASH_TREE, this, null);
                        hashRingString = new String(hashRingData1);
                        hashRing = new ECSHashRing(hashRingString);
                        ECSNode self_n = hashRing.getNodeByHash(MD5.HashInBI(getHostname() + ":" + port));
                        if (self_n.getFlag() == ECSNodeMessage.ECSNodeFlag.KV_TRANSFER) {
                            lockWrite();
                        }
                        logger.info("[KVServer] Hash Ring updated");

                        if (dataReplicationManager != null) {
                            dataReplicationManager.update(hashRing);
                        }
                    } catch (KeeperException | InterruptedException | IOException e) {
                        logger.info("[KVServer] Unable to update the metadata node");
                        e.printStackTrace();
                    }
                }
            }, null);
            logger.debug("[KVServer] Hash Ring found");
            hashRingString = new String(hashRingData);
            hashRing = new ECSHashRing(hashRingString);

        } catch (InterruptedException | KeeperException e) {
            logger.debug("[KVServer] Unable to get metadata info");
            e.printStackTrace();
        }

        try {
            // set watcher on children
            zk.getChildren(this.zkNodePath, this, null);
            logger.debug("[KVServer] Set up watcher on " + zkNodePath);
        } catch (InterruptedException | KeeperException e) {
            logger.error("[KVServer] Unable to get set watcher on children");
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
    public boolean moveData(String[] range, int target_port) {

        if (range.length < 2) {
            logger.debug("[KVServer] Invalid hash range for move data");
            return false;
        }

        this.lockWrite();

        //return byte array of Data
        String DataResult;
        try {
            DataResult = DB.getPreMovedData(range);
            if (DataResult == null || DataResult.equals("")) {
                logger.warn("[KVServer] No data to transfer!");
                return false;
            }
        } catch (Exception e) {
            this.unlockWrite();
            logger.error("[KVServer] Exceptions in getting moved data");
            return false;
        }

        try {
            KVStore tempClient = new KVStore(ECS.ZK_HOST, target_port);
            tempClient.connect();

            // TODO: Needs a message here and check its status
            TextMessage result = tempClient.sendMovedData(DataResult);
            tempClient.disconnect();

            if (result.getMsg().equals("Transferring_Data_SUCCESS")) {
                DB.deleteKVPairByRange(range);
                this.unlockWrite();
                logger.debug("[KVServer] Transfer success at senders!");
                return true;
            } else if (result.getMsg().equals("Transferring_Data_ERROR")) {
                logger.debug("[KVServer] Transfer failure at senders!");
            }

            this.unlockWrite();
            return false;

        } catch (Exception e) {
            this.unlockWrite();
            logger.error("[KVServer] Exceptions in sending moved data");
            return false;

        }
    }

//    public ECSHashRing getMetaData() {
//        return hashRing;
//    }

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
                    //ZK.update(path, "".getBytes());
                    logger.info("[KVServer] Server started!");
                    break;

                case SHUT_DOWN:
                    ZK.deleteNoWatch(path);
                    //ZK.update(path, "".getBytes());
                    logger.info("[KVServer] Server shutdown!");
                    shutdown();
                    break;

                case START:
                    ZK.deleteNoWatch(path);
                    //ZK.update(path, "".getBytes());
                    this.start();
                    logger.info("[KVServer] Server start taking request!");
                    break;
                case STOP:
                    ZK.deleteNoWatch(path);
                    //ZK.update(path, "".getBytes());
                    this.stop();
                    logger.info("[KVServer] Server stopped!");
                    break;
                case KV_RECEIVE:
                    ZK.deleteNoWatch(path);
                    break;

                case KV_TRANSFER:
                    assert tokens.length == 4;
                    int target = Integer.parseInt(tokens[1]);
                    String[] range = new String[]{
                            tokens[2], tokens[3]};

                    // send data
                    if (moveData(range, target)) {
                        logger.info("[KVServer] move data success!");
                    } else {
                        logger.warn("[KVServer] move data failure!");
                    }

                    updateTransferProgress(100);

                    String msgPath = ZK_SERVER_PATH + "/" + port + ECS.ZK_OP_PATH;

                    if (zk.exists(msgPath, false) == null) {
                        ZKAPP.create(msgPath, IECSNode.ECSNodeFlag.TRANSFER_FINISH.name().getBytes());
                    } else {
                        ZK.update(msgPath, IECSNode.ECSNodeFlag.TRANSFER_FINISH.name().getBytes());
                    }

                    String targetPath = ZK_SERVER_PATH + "/" + target + ECS.ZK_OP_PATH;

                    if (zk.exists(targetPath, false) == null) {
                        ZKAPP.create(targetPath, IECSNode.ECSNodeFlag.TRANSFER_FINISH.name().getBytes());
                    } else {
                        ZK.update(targetPath, IECSNode.ECSNodeFlag.TRANSFER_FINISH.name().getBytes());
                    }
                    unlockWrite();
                    break;

                case DELETE:

                    //this.lockWrite();
                    // delete hashRange??
                    this.unlockWrite();
                    this.clearCache();
                    ZK.deleteNoWatch(path);
                    break;

            }

            if (this.isRunning())
                zk.getChildren(zkNodePath, this, null);

        } catch (KeeperException | InterruptedException e) {
            logger.debug("[KVStore] Unable to process the watcher event");
            e.printStackTrace();
        }
    }

//    public void update(ECSHashRing hashRing) {
//        ECSNode node = hashRing.getNodeByName(this.serverName);
//        if (node == null) {
//            // server idle or shutdown
//            this.clearStorage();
//        }
//    }

    public String getHashRingStr() {
        return hashRingString;
    }

    //    public ECSHashRing getHashRing() {
//        return hashRing;
//    }
//
    public boolean isResponsible(String key, String cmd) {

        ECSNode node = hashRing.getNodeByHash(MD5.HashInBI(key));
        ECSNode thisServer = hashRing.getNodeByServerName(this.name);
        if (node == null) {
            logger.error("[KVStore] No node in hash ring is responsible for key " + key);
            return false;
        }
        boolean responsible = node.getNodePort() == port;

        if (cmd.equals("GET") || cmd.equals("PUT_REPLICATE")) {
            Collection<ECSNode> replicationNodes =
                    hashRing.getReplicas(node);
            responsible = responsible || replicationNodes.contains(thisServer);

        }
        return responsible;
    }

    public boolean receiveTransferredData(String data) {
        lockWrite();
        String msgPath = ZK_SERVER_PATH + "/" + port + "/op";
        DB.receiveTransferdData(data);
        unlockWrite();

        try {
            ZK.update(msgPath, IECSNode.ECSNodeFlag.TRANSFER_FINISH.name().getBytes());
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void updateTransferProgress(int transferProgress) {
        try {
            String msg = new String(zk.getData(zkNodePath + ECS.ZK_OP_PATH, false, null));

            if (msg.equals(ECSNodeMessage.ECSNodeFlag.TRANSFER_FINISH.name()))
                return;

            msg = msg.concat(Constants.DELIMITER + transferProgress);

            Stat stat = zk.setData(zkNodePath + ECS.ZK_OP_PATH, msg.getBytes(),
                    zk.exists(zkNodePath + ECS.ZK_OP_PATH, false).getVersion());
            logger.info("[KVServer] Update TransferProgress: " + transferProgress);

        } catch (InterruptedException | KeeperException e) {
            logger.info("[KVServer] Unable to update progress");
            e.printStackTrace();
        }

    }

    public KVServerDataReplicationManager getDataReplicationManager() {
        return this.dataReplicationManager;

    }
}
