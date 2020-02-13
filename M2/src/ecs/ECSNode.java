package ecs;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.TreeMap;

import app_kvServer.DataObjects.MetaData;

public class ECSNode implements IECSNode, Serializable {

    private String name;
    private String host;
    private int port;
    private BigInteger startHash;
    private BigInteger endHash;
    private TreeMap<BigInteger, MetaData> metaData;
//    private String cacheStrategy;
//    private int cacheSize;
    private ECSNodeFlag flag;

    public ECSNode(String name, String host, int port){
        this.name = name;
        this.host = host;
        this.port = port;
        this.startHash = null;
        this.endHash = null;
        metaData = new TreeMap<>();
        flag = ECSNodeFlag.STOP;
    }

    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    public String getNodeName(){
        return name;
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost(){
        return host;
    }

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort(){
        return port;
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange(){
        String[] range = new String[2];
        range[0] = startHash.toString();
        range[1] = endHash.toString();
        return range;
    }

    /**
     * This sets the new hash range for this ECS node
     * @param start
     * @param end
     */
    public void setHashRanges(BigInteger start, BigInteger end){
        this.startHash = start; //start.add(new BigInteger(("1")));  // TODO
        this.endHash = end;
    }


    public void setFlag(ECSNodeFlag newFlag){
        flag = newFlag;
    }

    public ECSNodeFlag getFlag(){
        return flag;
    }

    public TreeMap<BigInteger, MetaData> getMetaData(){
        return metaData;
    }





}
