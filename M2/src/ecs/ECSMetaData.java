package ecs;

import shared.HashingFunction.MD5;
import shared.messages.KVMessage;

import java.math.BigInteger;

public class ECSMetaData {

    private String name;
    private String host = null;
    private int port = -1;
    private int cacheSize;
    private String replacementStrategy;
    // start hash is hash(host:port)
    private BigInteger startHash;
    private BigInteger endHash;
    private KVMessage.ServerStateType state = KVMessage.ServerStateType.STOPPED;

    public ECSMetaData(String name, String host, int port) {

        this.name = name;
        this.host = host;
        this.port = port;
        this.endHash= MD5.HashFromHostAddress(host, port);
    }

    public ECSMetaData(int cacheSize, String replacementStrategy) {

        this.cacheSize = cacheSize;
        this.replacementStrategy = replacementStrategy;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getHost() {
        return this.host;
    }

    public void setCacheSize(int cacheSize) {
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

    public void setStartHash(BigInteger startHash) {
        this.startHash = startHash;
    }

    public BigInteger getStartHash() { return this.startHash; }

    public BigInteger getEndHash() {
        return this.endHash;
    }

    public void setHashRange(BigInteger startHash, BigInteger endHash) {
        this.startHash = startHash;
        this.endHash = endHash;
    }

    public BigInteger[] getHashRange() {
        return new BigInteger[]{
                this.startHash,
                this.endHash
        };
    }

//    public void setPort(int port) {
//        this.port = port;
//    }

    public int getPort() {
        return this.port;
    }

    public void setServerStateType(KVMessage.ServerStateType state) {
        this.state = state;
    }

    public KVMessage.ServerStateType getServerStateType() {
        return this.state;
    }
}
