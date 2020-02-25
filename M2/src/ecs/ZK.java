package ecs;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZK {
    private static ZooKeeper zk;
    private static final String ZK_HOST = "localhost";
    private static final int ZK_TIMEOUT = 2000;
    private int ZK_PORT;
    private static final String ZK_ROOT_PATH = "/root";
    private static final String ZK_SERVER_PATH = "/server";
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZooKeeper connect() throws IOException, IllegalStateException {
        zk = new ZooKeeper(ZK_HOST, ZK_TIMEOUT, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        try {
            connectedSignal.await();
        } catch (InterruptedException e) {
        }
        return zk;
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public void create(String path, byte[] data) throws KeeperException, InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public static byte[] read (String path) throws KeeperException, InterruptedException {
        return zk.getData(path, true, zk.exists(path, true));
    }

    public static void update (String path, byte[] data) throws KeeperException, InterruptedException {
        zk.setData(path, data, zk.exists(path, true).getVersion());
    }

    public static void delete(String path) throws KeeperException, InterruptedException {
        zk.delete(path, zk.exists(path, true).getVersion());
    }

    // ACL: access control list
    // authentication method
    // this returns {<scheme>, <who can access>}
    public static List<ACL> getacl (String path) throws KeeperException, InterruptedException {
        return zk.getACL(path, zk.exists(path, true));
    }

}
