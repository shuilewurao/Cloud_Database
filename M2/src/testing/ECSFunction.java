package testing;

import client.KVStore;
import app_kvServer.KVServer;
import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.ECSHashRing;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import junit.framework.TestCase;

import app_kvECS.ECSClient;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.math.BigInteger;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ECSFunction extends TestCase {
    private Collection<KVStore> kvClient = new ArrayList();
	private Exception ex = null;
	private ECS ecs;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private  int numGetClients = 5;

    public void setUp() {
    	try{
     		ecs = new ECS("./ecs.config");
            ecs.addNodes(3, "FIFO", 10);
            ecs.start_script();
            ecs.start();
    	}catch(Exception e){
    		//System.out.println("ECS Test error "+e);
    	}
    }

    public void tearDown() {
    	try{
            ecs.shutdown();
    	}catch(Exception e){
    		//System.out.println("ECS Test error "+e);
    	}
    }

    @Test
     public void test_connection()
     {
            Map<String, IECSNode> nodes = ecs.getNodes();

            for(IECSNode node : nodes.values())
            {
                Exception ex = null;
                System.out.println("Host " + node.getNodeHost() + "Port " + node.getNodePort());
                KVStore client = new KVStore(node.getNodeHost(), node.getNodePort());
                assertNotNull(client);
                try{
                    client.connect();
                }catch(Exception e){
                    ex = e;
                }
                kvClient.add(client);
                assertNull(ex);
            }
     }

     @Test
     public void test_putGetData()
     {
            Map<String, IECSNode> nodes = ecs.getNodes();
            int i = 0;
            try{
                for(KVStore client : kvClient)
                {
                    client.put("K" + i, "V" + i);
                }

                KVStore client = kvClient.iterator().next();

                for(int j = 0; j < 3; j++)
                {
                    String value = client.get("K" + j).getValue();
                    assert(value.equals("V"+j));
                }
            }catch(Exception e){
                ex = e;
            }
            assertNull(ex);
     }
}
