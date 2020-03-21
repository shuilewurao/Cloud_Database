package testing;

import client.KVStore;
import ecs.ECS;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import shared.Constants;
import shared.messages.KVMessage;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;

public class DataParserTest {
    private Logger logger = Logger.getRootLogger();

    private ECS ecs;
    private KVStore kvClient;
    private int port = 50005;

    private static List<String> msgs = DataParser.parseDataFrom("allen-p/inbox").subList(0, 10);

    public void setUp() throws IOException, InterruptedException, KeeperException {
        ecs = new ECS("./ecs.config");

        ecs.addNodes(1, "FIFO", 10);
        Thread.sleep(2000);

        ecs.start();
        Thread.sleep(2000);

    }

    public void tearDown() throws Exception {
        ecs.shutdown();
        Thread.sleep(2000);

    }

    @Test
    public void test01Put() throws Exception {

        logger.debug("[TEST] 1");

        kvClient = new KVStore("localhost", port);
        kvClient.connect();

        Thread.sleep(1000);


        for (String msg : msgs) {

            String[] tokens = msg.split("\\" + Constants.DELIMITER);

            KVMessage ret = kvClient.put(tokens[0], tokens[1]);
        }

        for (int i = 0; i < 64; i++) {
            int randIndex = new Random().nextInt(msgs.size() - 1);
            String msg1 = msgs.get(randIndex);
            String[] tokens1 = msg1.split("\\" + Constants.DELIMITER);
            KVMessage ret = kvClient.get(tokens1[0]);
            assertEquals(KVMessage.StatusType.GET_SUCCESS, ret.getStatus());
            assertEquals(tokens1[1], ret.getValue());
        }

        Thread.sleep(1000);
        kvClient.disconnect();
    }

}
