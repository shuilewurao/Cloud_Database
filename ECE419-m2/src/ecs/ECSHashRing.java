
package ecs;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import common.HashingFunction.MD5;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

public class ECSHashRing {

    private Logger logger = Logger.getRootLogger();
    private ECSNode root = null;

    private TreeMap<BigInteger, ECSNode> activeNodes = new TreeMap<>();

    public ECSHashRing() {
    }

    // TODO: check
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

    public void addNode(ECSNode node) {
        logger.debug("Current ring size: " + this.activeNodes.size());
        logger.debug("[ECSHashRing] Adding node: " + node.getNodeName());
        //printNode(node);

        ECSNode prevNode;
        ECSNode nextNode;

        // adding the first node
        if(getSize()==0){
            node.setHashRange(node.getNodeHash(), node.getNodeHash() );
        }else if(getSize()==1){
            if(this.activeNodes.firstEntry().getKey().compareTo(MD5.HashInBI(node.getNodeHash()))==-1){
                prevNode = this.activeNodes.firstEntry().getValue();
                node.setHashRange(prevNode.getNodeHash(), node.getNodeHash());
                prevNode.setHashRange(node.getNodeHash(), prevNode.getNodeHash());
                this.activeNodes.put(MD5.HashInBI(prevNode.getNodeHash()), prevNode);
            }else if(this.activeNodes.firstEntry().getKey().compareTo(MD5.HashInBI(node.getNodeHash()))==1){
                nextNode = this.activeNodes.firstEntry().getValue();
                this.activeNodes.put(MD5.HashInBI(nextNode.getNodeHash()), nextNode);
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
                this.activeNodes.put(MD5.HashInBI(nextNode.getNodeHash()), nextNode);
            }
        }

        this.activeNodes.put(MD5.HashInBI(node.getNodeHash()), node);

    }

    public ECSNode getNextNode(String hashName) {
        if (this.activeNodes.size() == 0)
            return null;
        BigInteger currKey = MD5.HashInBI(hashName);
        return getNextNode(currKey);
    }


    public ECSNode getNextNode(BigInteger currKey) {
        if (this.activeNodes.size() == 0)
            return null;
        if (this.activeNodes.lastKey().compareTo(currKey) == -1 ||
                this.activeNodes.lastKey().compareTo(currKey) == 0) {
            // return the first entry given the largest
            return this.activeNodes.firstEntry().getValue();
        }

        if (this.activeNodes.higherEntry(currKey) == null) {
            logger.debug("[ECSHashRing] " + currKey + " not found");
            logger.debug("[ECSHashRing]: "+this.activeNodes.keySet());
        }

        return this.activeNodes.higherEntry(currKey).getValue();
    }

    public ECSNode getPrevNode(String hashName) {
        if (this.activeNodes.size() == 0)
            return null;
        BigInteger currKey = MD5.HashInBI(hashName);
        return getPrevNode(currKey);
    }


    public ECSNode getPrevNode(BigInteger currKey) {
        if (this.activeNodes.size() == 0){
            return null;
        }

        if (this.activeNodes.firstKey().compareTo(currKey)==1
                || this.activeNodes.firstKey().compareTo(currKey)==0) {
            // return the last entry given the smallest
            return this.activeNodes.lastEntry().getValue();
        }

        if (this.activeNodes.lowerEntry(currKey) == null){
            logger.debug("[ECSHashRing] " + currKey + " not found");
        }

        return this.activeNodes.lowerEntry(currKey).getValue();

    }



    public String[] removeNode(ECSNode node) {

        logger.debug("[ECSHashRing] Removing node:");

        assert this.getSize() > 0 ;

        String[] hashRange = node.getNodeHashRange();

        if (this.getSize() == 1) {
            logger.debug("[ECSHashRing] only one node in the ring!");
        } else {
            ECSNode prevNode = this.getPrevNode(node.getNodeHash());

            ECSNode nextNode = this.getNextNode(node.getNodeHash());

            if (prevNode != null && nextNode != null) {
                nextNode.setHashRange(prevNode.getNodeHash(), nextNode.getNodeHash());
                this.activeNodes.put(MD5.HashInBI(nextNode.getNodeHash()), nextNode);
            }
        }

        this.activeNodes.remove(node.getNodeHash());
        return hashRange;
    }

    public ECSNode getNodeByKey(String key) {
        if (this.activeNodes.size() == 0)
            return null;
        BigInteger currKey = MD5.HashInBI(key);
        return getPrevNode(currKey);

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



    public void removeAll() {
        activeNodes = new TreeMap<>();
    }

}



/*
*/
/**
 * This class wrap the ECS Nodes and provide an LinkedList container
 * for adding and removing nodes
 *//*

public class ECSHashRing {
    private Logger logger = Logger.getRootLogger();
    private ECSNode root = null;
    private Integer size = 0;
    public static final String LOOP_ERROR_STR =
            "Mal-formed structure detected; potentially causing infinite loop";

    public Integer getSize() {
        return size;
    }

    public ECSHashRing() {
    }

    */
/**
     * Initialize hash ring with given nodes
     *
     * @param nodes ECSNodes
     *//*

    public ECSHashRing(Collection<RawECSNode> nodes) {
        for (RawECSNode node : nodes) {
            addNode(new ECSNode(node));
        }
    }

    @SuppressWarnings("unchecked")
    public ECSHashRing(String jsonData) {
        this((List<RawECSNode>) new Gson().fromJson(jsonData,
                new TypeToken<List<RawECSNode>>() {
                }.getType()));
    }

    */
/**
     * Get the node responsible for given key
     * Complexity O(n)
     *
     * @param key md5 hash string
     *//*

    public ECSNode getNodeByKey(String key) {
        BigInteger k = new BigInteger(key, 16);
        ECSNode currentNode = root;
        if (root == null) return null;
        Integer loopCounter = 0;

        while (true) {
            String[] hashRange = currentNode.getNodeHashRange();
            assert hashRange != null;

            BigInteger lower = new BigInteger(hashRange[0], 16);
            BigInteger upper = new BigInteger(hashRange[1], 16);

            if (upper.compareTo(lower) <= 0) {
                // The node is responsible for ring end
                if (k.compareTo(upper) <= 0 || k.compareTo(lower) > 0) {
                    break;
                }
            } else {
                if (k.compareTo(upper) <= 0 && k.compareTo(lower) > 0) {
                    break;
                }
            }
            currentNode = currentNode.getPrev();

            if (loopCounter > 2 * size)
                throw new HashRingException(LOOP_ERROR_STR);
        }
        return currentNode;
    }

    private static final BigInteger BIG_ONE = new BigInteger("1", 16);

    public ECSNode getNextNode(ECSNode n) {
        return getNextNode(n.getNodeHash());
    }

    public ECSNode getNextNode(String h) {
        BigInteger hash = new BigInteger(h, 16);
        hash = hash.add(BIG_ONE);
        return getNodeByKey(hash.toString(16));
    }

    */
/**
     * Add an node to hash ring
     * Complexity O(n)
     *
     * @param node ecsnode instance
     *//*

    public void addNode(ECSNode node) {
        logger.info("Adding node " + node);
        if (root == null) {
            root = node;
            root.setPrev(node);
        } else {
            ECSNode loc = getNodeByKey(node.getNodeHash());
            ECSNode prev = loc.getPrev();

            assert prev != null;

            if (node.getNodeHash().equals(loc.getNodeHash())) {
                throw new HashRingException(
                        "Hash collision detected!\nloc: " + loc + "\nnode: " + node);
            }

            node.setPrev(prev);
            loc.setPrev(node);
        }
        this.size++;
    }

    public void removeNode(ECSNode node) {
        removeNode(node.getNodeHash());
    }

    */
/**
     * Remove an node from hash ring based on the node's hash value
     * Complexity O(n)
     *
     * @param hash md5 hash string
     *//*

    public void removeNode(String hash) {
        logger.info("Removing node with hash " + hash);
        ECSNode toRemove = getNodeByKey(hash);
        if (toRemove == null) {
            throw new HashRingException(
                    "HashRing empty! while attempting to move item from it");
        }
        ECSNode next = getNextNode(hash);
        assert next != null;

        if (toRemove.equals(next) && root.getNodeHash().equals(hash)) {
            // remove the last element in hash ring
            root = null;
            size--;
            assert size == 0;
            return;
        } else if ((toRemove.equals(next) && !root.getNodeHash().equals(hash))
                || !(next.getPrev().equals(toRemove))) {
            throw new HashRingException("Invalid node hash value! (" + hash + ")\nnext: "
                    + next + "\nthis: " + toRemove + "\nnext->prev: " + next.getPrev());
        }

        next.setPrev(toRemove.getPrev());
        if (root.equals(toRemove)) {
            root = next;
        }
        this.size--;
    }

    */
/**
     * Remove all nodes in HashRing
     *//*

    public void removeAll() {
        if (root == null) return;

        ECSNode currentNode = root;
        Integer loopCounter = 0;
        while (currentNode != null) {
            ECSNode prev = currentNode.getPrev();
            currentNode.setPrev(null);
            currentNode = prev;
            loopCounter++;

            if (loopCounter > 2 * size) {
                throw new HashRingException(LOOP_ERROR_STR);
            }
        }
        size = 0;
        root = null;
    }

    @Override
    public String toString() {
        if (root == null)
            return "ECSHashRing{}";

        StringBuilder sb = new StringBuilder();
        sb.append("ECSHashRing{\n");

        ECSNode currentNode = root;
        Integer loopCounter = 0;
        while (true) {
            sb.append(currentNode);
            sb.append("\n");
            currentNode = currentNode.getPrev();
            if (currentNode.equals(root))
                break;

            loopCounter++;
            if (loopCounter > 2 * size)
                throw new HashRingException(LOOP_ERROR_STR);
        }
        sb.append("}");
        return sb.toString();
    }

    public boolean empty() {
        return this.root == null;
    }

    public static class HashRingException extends RuntimeException {
        public HashRingException(String msg) {
            super(msg);
        }
    }
}
*/
