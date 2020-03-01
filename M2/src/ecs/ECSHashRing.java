package ecs;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;
import shared.HashingFunction.MD5;

import java.math.BigInteger; // radix = 16 is the hexadecimal form

import java.util.*;
import java.util.stream.Collectors;

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
                new TypeToken<List<ECSNode>>() {
                }.getType());

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

    // find the responsible server node, or the node matching the hash
    public ECSNode getNodeByHash(BigInteger hash) {
        if (this.activeNodes.size() == 0)
            return null;


        if (this.activeNodes.lastKey().compareTo(hash)==-1){
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }

        if (this.activeNodes.ceilingEntry(hash).getValue() == null) {
            logger.debug("[ECSHashRing] " + hash + " not found");
        }

        return this.activeNodes.ceilingEntry(hash).getValue();
    }


    public ECSNode getNodeByName(String keyName) {

        logger.debug("[ECSHashRing] getting node using " + keyName);

        if (this.activeNodes.size() == 0) {
            logger.debug("[ECSHashRing] ring size is 0!");
            return null;
        }
        BigInteger hash = MD5.HashInBI(keyName);

        assert hash != null ;

        if (this.activeNodes.lastKey().compareTo(hash)==-1) {
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }

        if (this.activeNodes.ceilingEntry(hash).getValue() == null) {
            logger.debug("[ECSHashRing] " + keyName + " not found");
        }

        return this.activeNodes.ceilingEntry(hash).getValue();
    }


    public ECSNode getPrevNode(String hashName) {
        if (this.activeNodes.size() == 0)
            return null;
        BigInteger currKey = MD5.HashInBI(hashName);
        if (this.activeNodes.firstKey().compareTo(currKey)==1) {
            // return the last entry given the smallest
            return this.activeNodes.lastEntry().getValue();
        }

        if (this.activeNodes.lowerEntry(currKey).getValue() == null) {
            logger.debug("[ECSHashRing] " + hashName + " not found");
        }
        return this.activeNodes.lowerEntry(currKey).getValue();
    }

    public ECSNode getPrevNode(BigInteger currKey) {
        if (this.activeNodes.size() == 0){
            return null;
        }


        if (this.activeNodes.firstKey().compareTo(currKey)==1) {
            // return the last entry given the smallest
            return this.activeNodes.lastEntry().getValue();
        }

        if (this.activeNodes.lowerEntry(currKey) == null){
            logger.debug("[ECSHashRing] " + currKey + " not found");
        }

        return this.activeNodes.lowerEntry(currKey).getValue();

    }

    public ECSNode getNextNode(String hashName) {
        if (this.activeNodes.size() == 0)
            return null;
        BigInteger currKey = MD5.HashInBI(hashName);
        if (this.activeNodes.lastKey().equals(currKey)) {
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }

        if (this.activeNodes.higherEntry(currKey).getValue() == null) {
            logger.debug("[ECSHashRing] " + hashName + " not found");
        }

        return this.activeNodes.higherEntry(currKey).getValue();

    }

    public ECSNode getNextNode(BigInteger currKey) {
        if (this.activeNodes.size() == 0)
            return null;
        if (this.activeNodes.lastKey().compareTo(currKey) == -1) {
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }

        if (this.activeNodes.higherEntry(currKey).getValue() == null) {
            logger.debug("[ECSHashRing] " + currKey + " not found");
        }

        return this.activeNodes.higherEntry(currKey).getValue();

    }

    public void addNode(ECSNode node) {
        logger.debug("Current ring size: " + this.activeNodes.size());
        logger.debug("[ECSHashRing] Adding node: " + node.getNodeName());
        printNode(node);

        ECSNode prevNode;
        ECSNode nextNode;

        // adding the first node
        if(getSize()==0){
            node.setHashRange(node.getNodeHash(), node.getNodeHash() );
        }else if(getSize()==1){
            if(this.activeNodes.firstEntry().getKey().compareTo(node.getNodeHash())==-1){
                prevNode = this.activeNodes.firstEntry().getValue();
                node.setHashRange(prevNode.getNodeHash(), node.getNodeHash());
                prevNode.setHashRange(node.getNodeHash(), prevNode.getNodeHash());
                this.activeNodes.put(prevNode.getNodeHash(), prevNode);
            }else if(this.activeNodes.firstEntry().getKey().compareTo(node.getNodeHash())==1){
                nextNode = this.activeNodes.firstEntry().getValue();
                this.activeNodes.put(nextNode.getNodeHash(), nextNode);
            }else{
                logger.error("A collision on hash ring");
                return;
            }
        }else{
            prevNode = this.getPrevNode(node.getNodeHash());
            if (prevNode != null) {
                node.setHashRange(prevNode.getNodeHash(), node.getNodeHash());
            }

            nextNode = this.getNextNode(node.getNodeHash());
            if (nextNode != null) {
                nextNode.setHashRange(node.getNodeHash(), nextNode.getNodeHash());
                this.activeNodes.put(nextNode.getNodeHash(), nextNode);
            }
        }

        this.activeNodes.put(node.getNodeHash(), node);

    }

    public String[] removeNode(ECSNode node) {

        logger.debug("[ECSHashRing] Removing node:");

        printNode(node);

        assert this.getSize() > 0 ;

        String[] hashRange = node.getNodeHashRange();

        if (this.getSize() == 1) {
            // TODO: ???????
            logger.debug("[ECSHashRing] only one node in the ring!");
        } else {
            ECSNode prevNode = this.getPrevNode(node.getNodeHash());

            ECSNode nextNode = this.getNextNode(node.getNodeHash());

            if (prevNode != null && nextNode != null) {
                nextNode.setHashRange(prevNode.getNodeHash(), nextNode.getNodeHash());
                this.activeNodes.put(nextNode.getNodeHash(), nextNode);
            }
        }

        this.activeNodes.remove(node.getNodeHash());
        return hashRange;
    }

    public void printNode(ECSNode node) {
        logger.debug("\t\tnode name: " + node.getNodeName());
        logger.debug("\t\tnode host: " + node.getNodeHost());
        logger.debug("\t\tnode hash: " + node.getNodeHash());
    }

    public void printAllNodes() {
        for(Map.Entry<BigInteger,ECSNode> entry : this.activeNodes.entrySet()) {
            ECSNode node  = entry.getValue();

            logger.debug("\t\tnode name: " + node.getNodeName());
            logger.debug("\t\tprev node: " + getPrevNode(node.name).getNodeName());
            logger.debug("\t\tnext node: " + getNextNode(node.name).getNodeName());
            logger.debug("\t\tnode start hash: " + node.getNodeHashRange()[0]);
            logger.debug("\t\tnode end hash: " + node.getNodeHashRange()[1]);
            logger.debug("\t\t**************************************************");
        }
    }


    public String getHashRingJson() {
        List<ECSNode> activeNodes = getActiveNodes().values().stream()
                .collect(Collectors.toList());
        return new Gson().toJson(activeNodes);
    }
}
