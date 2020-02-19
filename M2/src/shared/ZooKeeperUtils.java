package shared;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public interface ZooKeeperUtils {

    public static final String ZK_METADATA_ROOT = "/metadata";

    public static Logger logger = Logger.getRootLogger();


    public static boolean update(ZooKeeper zk, String dir, byte[] newData) {
        try {
            Stat exists = zk.exists(dir, false);
            if (exists == null) {
                zk.create(dir, newData,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                zk.setData(ZK_METADATA_ROOT, newData,
                        exists.getVersion());
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted");
            return false;
        } catch (KeeperException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }


}
