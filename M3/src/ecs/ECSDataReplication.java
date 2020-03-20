package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import shared.Constants;

import java.util.Arrays;
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


    private CountDownLatch sig = null;
    //private CountDownLatch connectedSignal = new CountDownLatch(1);

    private TransferType type;

    public enum TransferType {
        COPY, // keep local copy after transmission
        DELETE // delete the content
    }


    public ECSDataReplication(ECSNode deleter, String[] hashRange) {
        this.hashRange = hashRange;
        this.type = TransferType.DELETE;
        this.sender = deleter;
    }

    public ECSDataReplication(ECSNode sender, ECSNode receiver, String[] hashRange) {
        this.hashRange = hashRange;
        this.type = TransferType.COPY;
        this.sender = sender;
        this.receiver = receiver;
    }

    private boolean init() throws InterruptedException, KeeperException {

        boolean sigWait;


        boolean ack;

        //sig = new CountDownLatch(1);
        //broadcast(ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH, IECSNode.ECSNodeFlag.KV_RECEIVE.name(), sig);

        //sigWait = sig.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS); // TODO
//        boolean ack = true;
//        if (!sigWait) {
//            if (zk.exists(ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH, false) != null) {
//                ack = false;
//            }
//        }

//        if (!ack) {
////            logger.error("[ECSDR] Failed to ack receiver of data " + receiver.name);
////            logger.error("[ECSDR] hash range is " + hashRange[0] + " to " + hashRange[1]);
//            return false;
//        }

        logger.info("[ECSDR] receiver " + receiver.name);

        String to_msg = IECSNode.ECSNodeFlag.KV_TRANSFER.name() +
                Constants.DELIMITER + receiver.getNodePort()
                + Constants.DELIMITER + hashRange[0]
                + Constants.DELIMITER + hashRange[1];

        sig = new CountDownLatch(1);

        broadcast(ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH, to_msg, sig);

        sigWait = sig.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS); // TODO
        ack = true;
        if (!sigWait) {
            logger.debug("[ECSDR] init reaches timeout...");
            if (zk.exists(ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH, false) != null) {
                ack = false;
            }
        }

        if (!ack) {
//            logger.error("[ECSDR] Failed to ack receiver of data " + receiver.name);
//            logger.error("[ECSDR] hash range is " + hashRange[0] + " to " + hashRange[1]);
            return false;
        }

        logger.info("[ECSDR] sender " + sender.name);
        return true;
    }

    public boolean start(ZooKeeper zk) throws InterruptedException, KeeperException {
        switch (this.type) {
            case DELETE:
                return delete(zk);
            case COPY:
                return copy(zk);
        }
        return false;
    }

    private boolean delete(ZooKeeper zk) throws InterruptedException, KeeperException {

        this.zk = zk;
        sig = new CountDownLatch(1);

        logger.info("[ECSDR] offloading " + sender.name);
        String msg = IECSNode.ECSNodeFlag.DELETE.name()
                + Constants.DELIMITER + hashRange[0]
                + Constants.DELIMITER + hashRange[1];
        broadcast(ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH, msg, sig);

        boolean sigWait = sig.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS);
        boolean ack = true;
        if (!sigWait) {
            logger.debug("[ECSDR] delete reaches timeout...");
            if (this.zk.exists(ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH, false) != null) {
                ack = false;
            }
        }
        //            logger.error("[ECSDR] Failed to ack receiver of data " + receiver.name);
        //            logger.error("[ECSDR] hash range is " + hashRange[0] + " to " + hashRange[1]);
        return ack;
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

        // Start listening sender's progress
        //String toPath = ECS.ZK_SERVER_PATH + "/" + this.receiver.port + ECS.ZK_OP_PATH;
        String fromPath = ECS.ZK_SERVER_PATH + "/" + this.sender.port + ECS.ZK_OP_PATH;

        while (true) {

            String fromMsg = new String(ZK.readNullStat(fromPath));
            //String toMsg = new String(ZK.readNullStat(toPath));
            //if (fromMsg.equals(IECSNode.ECSNodeFlag.TRANSFER_FINISH.name()) && toMsg.equals(IECSNode.ECSNodeFlag.TRANSFER_FINISH.name())) {
            if(fromMsg.equals(IECSNode.ECSNodeFlag.TRANSFER_FINISH.name())) {
//                if (!(zk.exists(toPath, false) == null))
//                    ZK.deleteNoWatch(toPath);
                if (!(zk.exists(fromPath, false) == null))
                    ZK.deleteNoWatch(fromPath);
                break;
            }
        }

        logger.info("[ECSDR] Completed sent from "+this.sender);
        return true;
    }


    public void broadcast(String msgPath, String msg, CountDownLatch sig) {

        try {
            if (this.zk.exists(msgPath, this) == null) {
                this.zk.create(msgPath, msg.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                logger.warn("[ECS] " + msgPath + " already exists... updating to "+msg+" and deleting children...");
                this.zk.setData(msgPath, msg.getBytes(), this.zk.exists(msgPath, true).getVersion());
                List<String> children = this.zk.getChildren(msgPath, false);
                for (String child : children)
                    this.zk.delete(msgPath + "/" + child, this.zk.exists(msgPath + "/" + child, true).getVersion());
            }

            //if (this.zk.exists(msgPath, this) == null) {
                sig.countDown();
                //logger.debug("[ECS] Unable to create path " + msgPath);
            //}
        } catch (KeeperException | InterruptedException e) {
            logger.error("[ECS] Exception sending ZK msg at " + msgPath + ": " + e);
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType().equals(Event.EventType.NodeDataChanged)) {
//            try {
//                if (!senderComplete) {
//                    checkSender();
//                } else if (!receiverComplete) {
//                    checkReceiver();
//                }
                logger.debug("[ECSDR]: process() "+event);
//            } catch (KeeperException e) {
//                logger.error(e.getMessage());
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        } else {
            logger.warn("[ECSDR] " + event);
        }
    }

    @Override
    public String toString() {
        return "[ECSDR] ECSDataTransferIssuer{" +
                "sender = " + sender +
                ", receiver = " + receiver +
                ", hashRange = " + Arrays.toString(hashRange) +
                ", typ e= " + type +
                '}';
    }
}