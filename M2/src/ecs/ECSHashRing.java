package ecs;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;
import shared.HashingFunction.MD5;
import shared.ZooKeeperUtils;

import java.math.BigInteger; // radix = 16 is the hexadecimal form

import java.util.*;

/**
 * This class builds the Logical Hash Ring using a TreeMap
 * The hash ring is consisted of ECSNodes that can be configurable
 */
public class ECSHashRing {

    private Logger logger = Logger.getRootLogger();
    private TreeMap<BigInteger, ECSNode> activeNodes = new TreeMap<>();

    // private ECSNode root = null;
    private int ringSize = 0;

    public ECSHashRing() {
    }

    // TODO: Check
    public ECSHashRing(String jsonData) {
        Collection<ECSNode> nodes = new Gson().fromJson(
                jsonData,
                new TypeToken<List<ECSNode>>() {}.getType());

        for (ECSNode node : nodes) {
            addNode(new ECSNode(node));
        }
    }



    public int getSize() {
        return ringSize;
    }

    public TreeMap<BigInteger, ECSNode> getActiveNodes() {
        return activeNodes;
    }

    // TODO
    public ECSNode getNodeByHostPort(ECSNode node) {
        return null;

    }

    public ECSNode getNodeByHash(BigInteger hash) {
        if(this.activeNodes.lastKey() == hash){
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }
        return this.activeNodes.ceilingEntry(hash).getValue();
    }


    // TODO
    public ECSNode getNodeByName(String name){
        BigInteger hash = MD5.HashInBI(name);
        if(this.activeNodes.lastKey() == hash){
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }
        return this.activeNodes.ceilingEntry(hash).getValue();
    }

    public ECSNode getPrevNode(String nodeName) {
        BigInteger currKey = MD5.HashInBI(nodeName);
        if(this.activeNodes.firstKey() == currKey){
            // return the last entry given the smallest
            return this.activeNodes.lastEntry().getValue();
        }
        return this.activeNodes.lowerEntry(currKey).getValue();

    }

    public ECSNode getNextNode(String nodeName) {
        BigInteger currKey = MD5.HashInBI(nodeName);
        if(this.activeNodes.lastKey() == currKey){
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }
        return this.activeNodes.lowerEntry(currKey).getValue();

    }

    public void addNode(ECSNode node) {
        logger.info("[ECSHashRing] Adding node: " + node.getNodeName());
        printNode(node);

        this.activeNodes.put(node.getNodeHash(), node);
        this.ringSize = this.activeNodes.size();
    }

    public void removeNode(ECSNode node) {

        // TODO make sure removing a node that exists

        logger.info("[ECSHashRing] Removing node:");

        printNode(node);


        this.activeNodes.remove(node);
        this.ringSize=this.activeNodes.size();
    }

    public void printNode(ECSNode node) {
        logger.info("    node name: " + node.getNodeName());
        logger.info("    node host: " + node.getNodeHost());
    }
}
