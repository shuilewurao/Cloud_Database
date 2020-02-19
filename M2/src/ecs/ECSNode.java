package ecs;

import shared.messages.KVMessage;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ECSNode implements IECSNode {

    private static final String HASH_DELIMITER = ":";

    private ECSMetaData metaData;

    protected String name;
    protected String host;
    protected Integer port;

    /*

    Server responsible for KV pairs between next to current

     */

    private ECSNode nextNode;
    //private ECSNode prevNode;
    private ECSNodeMessage.ECSNodeFlag flag = ECSNodeMessage.ECSNodeFlag.STOP;

    public ECSNode(ECSNode n) {
        this(n.name, n.host, n.port);
    }


    public ECSNode(String name, String host, int port) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.metaData = new ECSMetaData(name, host, port);
    }

    // TODO
    /*
     * ECSNode should also cover key value pair??
     * */


    @Override
    public String getNodeName() {
        return this.metaData.getName();
    }

    @Override
    public String getNodeHost() {
        return this.metaData.getHost();
    }

    @Override
    public int getNodePort() {
        return this.metaData.getPort();
    }

    public void setNodeHash(BigInteger hash) {
        this.metaData.setStartHash(hash);
    }

    public BigInteger getNodeHash() {
        return this.metaData.getStartHash();
    }

    @Override
    public String[] getNodeHashRange() {
        BigInteger[] hashRange = this.metaData.getHashRange();
        return new String[]{
                hashRange[0].toString(),
                hashRange[1].toString()
        };
    }

    public void setServerStateType(KVMessage.ServerStateType stateType) {
        this.metaData.setServerStateType(stateType);
    }

    @Override
    public KVMessage.ServerStateType getServerStateType() {
        return this.metaData.getServerStateType();
    }

    // TODO: call this function
    public void setNextNode(ECSNode node) {
        this.nextNode = node;
    }

    @Override
    public ECSNode getNextNode() {
        return this.nextNode;
    }
//
//    public void setPrevNode(ECSNode node) {
//        this.prevNode = node;
//    }
//
//    public ECSNode getPrevNode() {
//        return this.prevNode;
//    }

    public void setFlag(ECSNodeMessage.ECSNodeFlag flag) {
        this.flag = flag;
    }

    public ECSNodeMessage.ECSNodeFlag getFlag() {
        return this.flag;
    }

    public ECSMetaData getMetaData() {
        return this.metaData;
    }


    public int getCacheSize() {
        return this.metaData.getCacheSize();
    }

    public void setCacheSize(int cacheSize) {
        this.metaData.setCacheSize(cacheSize);
    }

    public String getReplacementStrategy() {
        return this.metaData.getReplacementStrategy();
    }

    public void setReplacementStrategy(String replacementStrategy) {
        this.metaData.setReplacementStrategy(replacementStrategy);
    }
}
