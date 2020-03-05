package ecs;

import app_kvServer.IKVServer;
import shared.HashingFunction.MD5;

import java.math.BigInteger;

public class ECSNode extends ECSMetaData implements IECSNode {

    /*

    Server responsible for KV pairs between next to current

     */

    private ECSNodeMessage.ECSNodeFlag flag = ECSNodeMessage.ECSNodeFlag.SHUT_DOWN;

    public ECSNode(ECSNode n) {
        this(n.name, n.host, n.port);
    }


    public ECSNode(String name, String host, int port) {
        super(name, host, port);
    }


    @Override
    public String getNodeName() {
        return this.getName();
    }

    @Override
    public String getNodeHost() {
        return this.getHost();
    }

    @Override
    public int getNodePort() {
        return this.getPort();
    }

//    public void setHashRange(String startHash, String endHash) {
//        this.setHashRange(startHash, endHash);
//    }

    public String getNodeHash() {
        return this.getEndHash();
    }

    public BigInteger getNodeHashBI() {
        return MD5.HashInBI(this.getEndHash());
    }

    @Override
    public String[] getNodeHashRange() {
        return this.getHashRange();

    }


    public void setFlag(ECSNodeMessage.ECSNodeFlag flag) {
        this.flag = flag;
    }

    public ECSNodeMessage.ECSNodeFlag getFlag() {
        return this.flag;
    }


    public void shutdown() {
        setCacheSize(-1);
        setReplacementStrategy("");
        setFlag(ECSNodeMessage.ECSNodeFlag.SHUT_DOWN);
        setServerStateType(IKVServer.ServerStateType.SHUT_DOWN);
        clearStartHash();
    }

    public void init(int cacheSize, String replacementStrategy) {
        setCacheSize(cacheSize);
        setReplacementStrategy(replacementStrategy);
        setFlag(ECSNodeMessage.ECSNodeFlag.INIT);
        setServerStateType(IKVServer.ServerStateType.STOPPED);
    }
}
