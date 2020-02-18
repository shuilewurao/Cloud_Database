package ecs;

import org.apache.log4j.Logger;

import java.math.BigInteger; // radix = 16 is the hexadecimal form

import java.util.*;

/**
 * This class builds the Logical Hash Ring using a TreeMap
 * The hash ring is consisted of ECSNodes that can be configurable
 */
public class ECSHashRing {

    private Logger logger = Logger.getRootLogger();
    private LinkedList<ECSNode> activeNodes = new LinkedList<>();

    private ECSNode root = null;
    private int ringSize = 0;

    public ECSHashRing() {
    }

    public int getSize() {
        return ringSize;
    }

    public List<ECSNode> getActiveNodes() {
        return activeNodes;
    }

    public ECSNode getNodeByHostPort(ECSNode node) {
        return null;

    }

    public ECSNode getNodeByHash(BigInteger hash) {
        ECSNode curr = root;
        if (root == null) {
            return null;
        }

        while (true) {

            String[] hashRange = curr.getNodeHashRange();
            assert hashRange != null;

            BigInteger lowerBound = new BigInteger(hashRange[0], 16);
            BigInteger upperBound = new BigInteger(hashRange[1], 16);

            if (hash.compareTo(lowerBound) >= 0 && hash.compareTo(upperBound) <= 0) {
                break;
            }
            curr = curr.getNextNode();
        }
        return curr;
    }

    public ECSNode getPrevNode(String nodeName) {
        // TODO
        ECSNode prevNode = new ECSNode();
        if (ringSize < 1) {
            return null;
        } else {
            return prevNode;
        }

    }

    public ECSNode getNextNode(String nodeName) {
        // TODO
        ECSNode nextNode = new ECSNode();
        if (ringSize < 1) {
            return null;
        } else {
            return nextNode;
        }

    }

    public void addNode(ECSNode node) {
        logger.info("[ECSHashRing] Adding node: " + node.getNodeName());
        printNode(node);

        if (root == null) {
            root = node;
            // wrap around
            root.setNextNode(node);
            root.setPrevNode(node);
        } else {

            assert node.getNodeHost() != null;
            assert node.getNodePort() != -1;

            ECSNode currNode = getNodeByHash(node.getNodeHash());
            ECSNode nextNode = currNode.getNextNode();

            /*

                -->
            node * * Next
            *          *
            *          *
            Curr * * * *
                <--

            * */
            node.setNextNode(nextNode);
            node.setPrevNode(currNode);

            currNode.setNextNode(node);
            nextNode.setPrevNode(node);

        }

        this.activeNodes.add(node);
        this.ringSize++;
    }

    public void removeNode(ECSNode node) {
        logger.info("[ECS_tmp Hash Ring] Removing node:");
        printNode(node);

        ECSNode nextNode = node.getNextNode();
        ECSNode prevNode = node.getPrevNode();

        if (nextNode.equals(node) && nextNode.equals(prevNode)) {
            // one node in the ring

            assert ringSize == 1;
            root = null;
        } else if (nextNode.equals(prevNode)) {
            // more than one node in the ring

            nextNode.setPrevNode(prevNode);
            prevNode.setNextNode(nextNode);
        }


        this.activeNodes.remove(node);
        this.ringSize--;
    }

    public void printNode(ECSNode node) {
        logger.info("    node name: " + node.getNodeName());
        logger.info("    node host: " + node.getNodeHost());
    }
}
