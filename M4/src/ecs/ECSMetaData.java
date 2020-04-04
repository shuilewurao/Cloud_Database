package ecs;

import app_kvServer.IKVServer;
import shared.Constants;

public class ECSMetaData {

    protected String name;
    protected String host;
    protected int port;

    private int cacheSize = -1;
    private String replacementStrategy = "";
    private String startHash = "";
    // start hash is hash(host:port)
    private String endHash;

    private IKVServer.ServerStateType state = IKVServer.ServerStateType.IDLE;

    public ECSMetaData(String name, String host, int port) {

        this.name = name;
        this.host = host;
        this.port = port;
        this.endHash = host + Constants.HASH_DELIMITER + port;
    }


    public String getName() {
        return this.name;
    }


    public String getHost() {
        return this.host;
    }

    protected void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public int getCacheSize() {
        return this.cacheSize;
    }

    public void setReplacementStrategy(String replacementStrategy) {
        this.replacementStrategy = replacementStrategy;
    }

    public String getReplacementStrategy() {
        return this.replacementStrategy;
    }

    public String getStartHash() {
        return this.startHash;
    }

    public String getEndHash() {
        return this.endHash;
    }

    public void setHashRange(String startHash, String endHash) {
        if (!(startHash == null)) {
            this.startHash = startHash;
        }

        if (!(endHash == null)) {
            this.endHash = endHash;
        }

    }

    public String[] getHashRange() {
        return new String[]{
                this.startHash,
                this.endHash
        };
    }

    public void clearStartHash() {
        this.startHash = "";
    }

    protected int getPort() {
        return this.port;
    }


    public void setServerStateType(IKVServer.ServerStateType state) {
        this.state = state;
    }

    public IKVServer.ServerStateType getServerStateType() {
        return this.state;
    }


    public ECSMetaData getMetaData() {
        return this;
    }
}
