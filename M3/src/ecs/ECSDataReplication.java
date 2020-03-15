package ecs;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import shared.Constants;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECSDataReplication implements Watcher {

    public static final Integer TIMEOUT = 5 * 1000;

    private static Logger logger = Logger.getRootLogger();

    private ZooKeeper zk;
    private ECSNode sender;
    private ECSNode receiver;
    private String[] hashRange;

    private boolean senderComplete = false;
    private boolean receiverComplete = false;

    private Integer senderProgress = -1;
    private Integer receiverProgress = -1;

    private String prompt;

    private CountDownLatch sig = null;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    private TransferType type;

    public enum TransferType {
        COPY, // keep local copy after transmission
        DELETE // delete the content
    }


    public ECSDataReplication(ECSNode deleter, String[] hashRange) {
        this.hashRange = hashRange;
        this.type = TransferType.DELETE;
        this.sender = deleter;
        this.prompt = this.sender.getNodeName() + " delete: ";
    }

    public ECSDataReplication(ECSNode sender, ECSNode receiver, String[] hashRange) {
        this.hashRange = hashRange;
        this.type = TransferType.COPY;
        this.sender = sender;
        this.receiver = receiver;
        this.prompt = sender.getNodeName() + "->" + receiver.getNodeName() + ": ";
    }

    private boolean init() throws InterruptedException, KeeperException {

        broadcast(ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH, IECSNode.ECSNodeFlag.KV_RECEIVE.name(), connectedSignal);

        boolean sigWait = connectedSignal.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS);
        boolean ack = true ;
        if (!sigWait) {
            if (zk.exists(ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH, false) != null) {
                ack = false;
            }
        }

        if (!ack) {
            logger.error("Failed to ack receiver of data " + receiver);
            logger.error("hash range is " + hashRange[0] + " to " + hashRange[1]);
            return false;
        }

        logger.info("Confirmed receiver node " + receiver.name);

        String to_msg = IECSNode.ECSNodeFlag.KV_TRANSFER.name() +
                Constants.DELIMITER + receiver.getNodePort()
                + Constants.DELIMITER + hashRange[0]
                + Constants.DELIMITER + hashRange[1];

        broadcast(ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH, to_msg, connectedSignal);

        boolean sigWait1 = connectedSignal.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS);
        ack = true ;

        logger.info("Confirmed sender node " + sender.name);
        return true;
    }

    public boolean start(ZooKeeper zk) throws InterruptedException, KeeperException {
        switch (this.type) {
            case DELETE:
                return delete(zk);
            case COPY:
                return copy(zk);
            default:
                logger.fatal("unrecognized transfer type");
                return false;
        }
    }

    private boolean delete(ZooKeeper zk) throws InterruptedException, KeeperException {
        broadcast(ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH, IECSNode.ECSNodeFlag.DELETE.name(), connectedSignal);

        boolean sigWait = connectedSignal.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS);
        boolean ack = true ;
        if (!sigWait) {
            if (zk.exists(ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH, false) != null) {
                ack = false;
            }
        }

        if (!ack) {
            logger.error("Failed to ack receiver of data " + receiver);
            logger.error("hash range is " + hashRange[0] + " to " + hashRange[1]);
            return false;
        }

        return true;
    }

    /**
     * Copy data in given range from one server to another
     *
     * @param zk zookeeper instance
     * @return successful or not
     * @throws InterruptedException transmission interrupted
     */
    private boolean copy(ZooKeeper zk) throws InterruptedException, KeeperException {
        this.zk = zk;
        if (!init()) return false;
        try {
            checkSender();
            if (senderComplete && receiverComplete) {
                logger.info(prompt + "transmission complete");
                return true;
            }
            zk.exists(ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH, this);
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            logger.error(e.getPath() + " : " + e.getResults());
            return false;
        }

        while (true) {
            Integer psender = senderProgress;
            Integer preciver = receiverProgress;

            sig = new CountDownLatch(1);
            sig.await(TIMEOUT, TimeUnit.MILLISECONDS);

            if (senderComplete && receiverComplete) {
                // Complete
                return true;
            } else if (receiverProgress.equals(preciver)
                    && senderProgress.equals(psender)) {
                if (senderProgress.equals(100))
                    // the action is complete
                    return true;
                // No data change
                // Must be a timeout
                logger.error("TIMEOUT triggered before receiving any progress on data transferring");
                logger.error("final progress " + senderProgress + "%");
                return false;
            }
        }
    }

    private void checkReceiver() throws KeeperException, InterruptedException {
        String msg = new String(zk.getData(ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH, false, null));

        if (msg.equals(ECSNodeMessage.ECSNodeFlag.TRANSFER_FINISH.name())) {
            receiverComplete = true;
            logger.info(prompt + "receiver side complete");
        } else {
            zk.exists(ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH, this);
        }
        if (sig != null) sig.countDown();
    }

    private void checkSender() throws KeeperException, InterruptedException {
        // Monitor sender
        String msg = new String(zk.getData(ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH, false, null));



        if (msg.equals(ECSNodeMessage.ECSNodeFlag.TRANSFER_FINISH.name())) {
            // Sender complete, now monitoring receiver
            senderComplete = true;
            logger.info(prompt + "sender side complete");
            checkReceiver();
        } else {
            // Continue listening for sender progress
            zk.exists(ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH, this);
            if (sig != null) sig.countDown();
        }
    }

    public void broadcast(String msgPath, String msg, CountDownLatch sig) {

        try {
            if (zk.exists(msgPath, this) == null) {
                zk.create(msgPath, msg.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                logger.warn("[ECS] " + msgPath + " already exists... updating and deleting children...");
                zk.setData(msgPath, msg.getBytes(), zk.exists(msgPath, true).getVersion());
                List<String> children = zk.getChildren(msgPath, false);
                for (String child : children)
                    zk.delete(msgPath + "/" + child, zk.exists(msgPath + "/" + child, true).getVersion());

            }

            if (zk.exists(msgPath, this) == null) {
                sig.countDown();
                logger.debug("[ECS] Unable to create path " + msgPath);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("[ECS] Exception sending ZK msg at " + msgPath + ": " + e);
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(Event.EventType.NodeDataChanged)) {
            try {
                if (!senderComplete) {
                    checkSender();
                } else if (!receiverComplete) {
                    checkReceiver();
                }
            } catch (KeeperException e) {
                logger.error(e.getMessage());
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            logger.warn("Other unexpected event monitored " + event);
            logger.warn("Continue listening for progress");
        }
    }

    @Override
    public String toString() {
        return "ECSDataTransferIssuer{" +
                "sender=" + sender +
                ", receiver=" + receiver +
                ", hashRange=" + Arrays.toString(hashRange) +
                ", type=" + type +
                '}';
    }
}