package ecs;

import app_kvServer.DataObjects.MetaData;
import shared.messages.KVMessage;

import java.math.BigInteger;
import java.util.TreeMap;

public interface IECSNode {

    public enum ECSNodeFlag{
        STOP,
        START,
        STATE_CHANGE,
        KV_TRANSFER,
        SHUT_DOWN,
        UPDATE,
        TRANSFER_FINISH,
        ERROR
    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

    public void setFlag(ECSNodeFlag flag);

    public ECSNodeFlag getFlag();

    public KVMessage.ServerStateType getServerStateType();

    public ECSNode getNextNode();

    public ECSMetaData getMetaData();

}
