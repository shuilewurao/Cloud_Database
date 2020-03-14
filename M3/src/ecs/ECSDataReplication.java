package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import shared.Constants;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECSDataReplication implements Watcher {

    private static Logger logger = Logger.getRootLogger();

    private ZK ZKAPP;
    private ZooKeeper zk;
    private ECSNode from;
    private ECSNode to;
    private String fromPath;
    private String toPath;
    private String[] hashRange;

    private boolean sendComplete = false;
    private boolean receiveComplete = false;

    public final CountDownLatch connectedSignal = new CountDownLatch(1);

    private ECSNodeMessage.ECSTransferType type;

    public ECSDataReplication(ECSNode sender, String[] hashRange) {
        this.from = sender;
        this.fromPath = ECS.ZK_SERVER_PATH + "/" + sender.getNodePort() + ECS.ZK_OP_PATH;
        this.hashRange = hashRange;
        this.type = ECSNodeMessage.ECSTransferType.DELETE;
    }

    public ECSDataReplication(ECSNode sender, ECSNode receiver, String[] hashRange) {
        this.from = sender;
        this.fromPath = ECS.ZK_SERVER_PATH + "/" + this.from.getNodePort() + ECS.ZK_OP_PATH;
        this.to = receiver;
        this.toPath = ECS.ZK_SERVER_PATH + "/" + this.to.getNodePort() + ECS.ZK_OP_PATH;
        this.hashRange = hashRange;
        this.type = ECSNodeMessage.ECSTransferType.COPY;
    }

    public boolean start(ZooKeeper zk, ZK ZKAPP) throws InterruptedException {
        switch (this.type) {
            case COPY:
                return copy(zk, ZKAPP);
            case DELETE:
                return delete(zk, ZKAPP);
            default:
                logger.error("[ECSDataReplication] Unknown type!");
                return false;
        }
    }

    private boolean copy(ZooKeeper zk, ZK ZKAPP) throws InterruptedException {
        this.zk = zk;
        this.ZKAPP = ZKAPP;

        String msg = ECSNodeMessage.ECSNodeFlag.SEND.name() +
                Constants.DELIMITER + this.to.getNodePort()
                + Constants.DELIMITER + this.hashRange[0]
                + Constants.DELIMITER + this.hashRange[1];

        broadcast(this.fromPath, msg, this.connectedSignal);

        try {
            checkSenderStatus();

            if (this.sendComplete && this.receiveComplete) {
                logger.info("[ECSDataReplication] Replication complete!");
                return true;
            }

            zk.exists(this.fromPath, this);
        } catch (Exception e) {
            e.printStackTrace();
        }

        while (true) {

            this.connectedSignal.await(Constants.TIMEOUT, TimeUnit.MILLISECONDS);

            if (this.sendComplete && this.receiveComplete) {
                return true;
            }
        }

    }


    private boolean delete(ZooKeeper zk, ZK ZKAPP) {
        this.zk = zk;
        this.ZKAPP = ZKAPP;

        String msg = ECSNodeMessage.ECSNodeFlag.DELETE.name() +
                Constants.DELIMITER + this.from.getNodePort()
                + Constants.DELIMITER + this.hashRange[0]
                + Constants.DELIMITER + this.hashRange[1];

        broadcast(this.fromPath, msg, this.connectedSignal);
        return true;
    }

    private void checkSenderStatus() throws KeeperException, InterruptedException {
        String msg = new String(ZK.readNullStat(this.fromPath));

        if (msg.equals(IECSNode.ECSNodeFlag.TRANSFER_FINISH.name())) {
            sendComplete = true;
            logger.info("[ECSDataReplication] Sender finishes transfer.");
            checkReceiverStatus();
        } else {
            this.zk.exists(fromPath, this);
            if (connectedSignal != null)
                connectedSignal.countDown();
        }
    }

    private void checkReceiverStatus() throws KeeperException, InterruptedException {
        String msg = new String(ZK.readNullStat(this.toPath));

        if (msg.equals(IECSNode.ECSNodeFlag.TRANSFER_FINISH.name())) {
            receiveComplete = true;
            logger.info("[ECSDataReplication] Receiver finishes transfer.");
        } else {
            this.zk.exists(fromPath, this);
            if (connectedSignal != null)
                connectedSignal.countDown();
        }
    }

    public void broadcast(String msgPath, String msg, CountDownLatch sig) {

        try {
            if (this.zk.exists(msgPath, this) == null) {
                this.ZKAPP.create(msgPath, msg.getBytes());
            } else {
                logger.warn("[ECSDataReplication] " + msgPath + " already exists... updating and deleting children...");
                ZK.update(msgPath, msg.getBytes());
            }

            if (this.zk.exists(msgPath, this) == null) {
                sig.countDown();
                logger.debug("[ECSDataReplication] Unable to create path " + msgPath);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("[ECSDataReplication] Exception sending ZK msg at " + msgPath + ": " + e);
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType().equals(Event.EventType.NodeDataChanged)) {
            try {
                if (!sendComplete) {
                    checkSenderStatus();
                } else if (!receiveComplete) {
                    checkReceiverStatus();
                }
            } catch (InterruptedException | KeeperException e) {
                e.printStackTrace();
            }
        }
    }
}
