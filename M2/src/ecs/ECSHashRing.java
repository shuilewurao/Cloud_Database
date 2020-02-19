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
        return this.activeNodes.size();
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
    public ECSNode getNodeByName(String keyName){
        BigInteger hash = MD5.HashInBI(keyName);
        if(this.activeNodes.lastKey() == hash){
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }
        return this.activeNodes.ceilingEntry(hash).getValue();
    }


    public ECSNode getPrevNode(String hashName) {
        BigInteger currKey = MD5.HashInBI(hashName);
        if(this.activeNodes.firstKey() == currKey){
            // return the last entry given the smallest
            return this.activeNodes.lastEntry().getValue();
        }
        return this.activeNodes.lowerEntry(currKey).getValue();

    }

    public ECSNode getPrevNode(BigInteger currKey) {
        if(this.activeNodes.firstKey() == currKey){
            // return the last entry given the smallest
            return this.activeNodes.lastEntry().getValue();
        }
        return this.activeNodes.lowerEntry(currKey).getValue();

    }

    public ECSNode getNextNode(String hashName) {
        BigInteger currKey = MD5.HashInBI(hashName);
        if(this.activeNodes.lastKey() == currKey){
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }
        // TODO: if works for a node not added yet
        return this.activeNodes.lowerEntry(currKey).getValue();

    }

    public ECSNode getNextNode(BigInteger currKey) {
        if(this.activeNodes.lastKey() == currKey){
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }
        return this.activeNodes.lowerEntry(currKey).getValue();

    }

    public void addNode(ECSNode node) {
        logger.info("[ECSHashRing] Adding node: " + node.getNodeName());
        printNode(node);

        ECSNode prevNode = this.getPrevNode(node.getNodeHash());
        if(prevNode != null){
            node.setNodeStartHash(prevNode.getNodeHash());
            this.activeNodes.put(prevNode.getNodeHash(), prevNode);
        }

        ECSNode nextNode = this.getNextNode(node.getNodeHash());
        if(nextNode != null){
            nextNode.setNodeStartHash(node.getNodeHash());
            this.activeNodes.put(nextNode.getNodeHash(), nextNode);
        }

        this.activeNodes.put(node.getNodeHash(), node);

    }

    public String[] removeNode(ECSNode node) {

        // TODO make sure removing a node that exists

        logger.info("[ECSHashRing] Removing node:");

        printNode(node);

        String[] hashRange = node.getNodeHashRange();

        ECSNode nextNode = this.getNextNode(node.getNodeHash());
        if(nextNode != null){
            nextNode.setNodeStartHash(node.getNodeStartHash());
            this.activeNodes.put(nextNode.getNodeHash(), nextNode);
        }

        this.activeNodes.remove(node);
        return hashRange;
    }

    public void printNode(ECSNode node) {
        logger.info("    node name: " + node.getNodeName());
        logger.info("    node host: " + node.getNodeHost());
    }


}
