package app_kvServer;

import app_kvServer.DataObjects.MetaData;

import java.math.BigInteger;
import java.util.*;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    public enum ServerStateType {
        IDLE,
        STOPPED,
        STARTED,
        SHUT_DOWN,
        ERROR
    }

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key);

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void putKV(String key, String value) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage();

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

    /**
     * ECS-related initialization
     */
    public void initKVServer(MetaData metadata, int cacheSize, String replacementStrategy);

    /**
     *  ECS-related start, start processing all client requests and all ECS requests
     */
    public void start();

    /**
     * ECS-related stop, reject all client requests and only process ECS requests
     */
    public void stop();

    /**
     *  ECS-related shutdown, exit the KVServer application
     */
    public void shutdown();

    /**
     * ECS-related lock, for write operations
     */
    public void lockWrite();

    /**
     * ECS-related unlock, for write operations
     */
    public void unLockWrite();

    /**
     * TODO: range
     * ECS-related moveData, move the given hashRange to the server going by the targetName
     */
    public boolean moveData(String[] range, String server);

    /**
     * ECS-related update, update the metadata repo of this server
     * @param metadata
     */
    public void update(MetaData metadata);

    public ServerStateType getServerState();

    public boolean isWriteLocked();

    public TreeMap<BigInteger, MetaData> getMetaData();



}
