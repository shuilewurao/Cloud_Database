package ecs;

import app_kvServer.IKVServer;
import shared.messages.KVMessage;

public interface IECSNode {

    enum ECSNodeFlag {
        INIT,
        STOP,
        START,
        STATE_CHANGE,
        KV_TRANSFER,
        KV_RECEIVE,
        SHUT_DOWN,
        UPDATE,
        TRANSFER_FINISH
    }


    /**
     * @return the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName();

    /**
     * @return the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost();

    /**
     * @return the port number of the node (ie 8080)
     */
    public int getNodePort();

    /**
     * @return array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

    public void setFlag(ECSNodeMessage.ECSNodeFlag flag);

    public ECSNodeMessage.ECSNodeFlag getFlag();

    public IKVServer.ServerStateType getServerStateType();

    // TODO
    //public ECSNode getNextNode();

    public ECSMetaData getMetaData();

}
