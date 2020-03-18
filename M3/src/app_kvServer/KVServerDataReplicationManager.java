package app_kvServer;

import ecs.ECS;
import ecs.ECSHashRing;
import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.messages.TextMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class KVServerDataReplicationManager {
    private Logger logger = Logger.getRootLogger();

    private ECSNode thisNode;
    private List<KVServerDataReplication> replicationList;
    private final String prompt = "[KVServerDRManagr] ";

    public KVServerDataReplicationManager(String name, String host, int port) {
        this.replicationList = new ArrayList<>();
        this.thisNode = new ECSNode(name, host, port);
    }

    public void update(ECSHashRing hashRing) throws IOException {
        ECSNode node = hashRing.getNodeByServerName(thisNode.getNodeName());

        if (node == null) {
            clear();
            return;
        }
        Collection<ECSNode> replicas = hashRing.getReplicas(node);
        List<KVServerDataReplication> toReplicate = new ArrayList<>();

        for (ECSNode n : replicas) {
            toReplicate.add(new KVServerDataReplication(n));
        }

        for (Iterator<KVServerDataReplication> it = replicationList.iterator(); it.hasNext(); ) {
            KVServerDataReplication r = it.next();

            if (!toReplicate.contains(r)) {
                logger.info(prompt + r.getServerName() + " disconnected from " + thisNode.getNodeName());
                r.disconnect();
                it.remove();
            }
        }

        for (KVServerDataReplication r : toReplicate) {
            if (!replicationList.contains(r)) {
                logger.info(prompt + r.getServerName() + " connects to " + thisNode.getNodeName());
                r.connect();
                replicationList.add(r);
            }
        }
    }

    public void forward(String cmd, String k, String v) throws IOException {
        for (KVServerDataReplication r : replicationList) {
            logger.debug(prompt + " data replication from " + this.thisNode.getNodeName() + " to " + r.getServerName());
            r.dataReplication(cmd, k, v);
        }
    }

    public void clear() {
        for (KVServerDataReplication r : replicationList) {
            r.disconnect();
        }
        replicationList.clear();
    }
}
