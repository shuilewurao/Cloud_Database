package testing;

import client.KVStore;
import app_kvServer.KVServer;
import ecs.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import junit.framework.TestCase;
import shared.messages.KVMessage;


import app_kvECS.ECSClient;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.math.BigInteger;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class ECSTest extends TestCase {
    private KVStore kvClient;
	private Exception ex = null;
	private ECS ecs;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private  int numGetClients = 5;

    public void setUp() {
    	try{
    		//ecsApp = new ECSClient("./ecs.config");
     		 ecs = new ECS("./ecs.config");
    	}catch(Exception e){
    		//System.out.println("ECS Test error "+e);
    	}
    }

    public void tearDown() {
    	try{
    		//ecsApp.shutdown();
    	}catch(Exception e){
    		//System.out.println("ECS Test error "+e);
    	}
    }

    @Test
     public void test_createECS()
     {
     		Exception ex = null;
     		try{
    			new ECS("./ecs.config");
     		}catch(Exception e){
     			ex = e;
     		}
     		assertNull(ex);
     }

    @Test
     public void test_addNode()
     {
     		Exception ex = null;
     		try{
    			ecs.addNodes(3, "FIFO", 10);
     		}catch(Exception e){
     			ex = e;
     		}
     		assertNull(ex);
     }

    @Test
     public void test_startNode()
     {
            Exception ex = null;
            try{
                ecs.start();
            }catch(Exception e){
                ex = e;
            }
            Map<String, IECSNode> nodes = ecs.getNodes();
            for(IECSNode node : nodes.values())
            {
                assertEquals(ECSNodeMessage.ECSNodeFlag.START, node.getFlag());
            }
            //assertNull(ex);
    }

     @Test
     public void test_removeNode()
     {
     		Exception ex = null;
     		try{
    			ecs.addNodes(2, "FIFO", 10);
     		}catch(Exception e){
     			ex = e;
     		}
    		Map<String, IECSNode> map = ecs.getNodes();

     		try{
	    		Collection<String> keys = map.keySet();
	    		ecs.removeNodes(keys);
     		}catch(Exception e){
     			ex = e;
     		}
     		assertNull(ex);
     }

     @Test
     public void test_removeNonExistNode()
     {
     		Exception ex = null;

     		try{
	    		Collection<String> keys = Arrays.asList("None-1","None-2");
	    		ecs.removeNodes(keys);
     		}catch(Exception e){
     			ex = e;
     		}
     		assertNotNull(ex);
     }

     @Test
     public void test_stop()
     {
     		Exception ex = null;
            try{
                ecs.addNodes(2, "FIFO", 10);
            }catch(Exception e){
                ex = e;
            }
     		try{
	    		ecs.stop();
     		}catch(Exception e){
     			ex = e;
     		}
            assertNull(ex);
            Map<String, IECSNode> nodes = ecs.getNodes();
            for(IECSNode node : nodes.values())
            {
                assertEquals(ECSNodeMessage.ECSNodeFlag.STOP, node.getFlag());
            }
     }

          @Test
     public void test_shutdown()
     {
            Exception ex = null;

            try{
                ecs.shutdown();
            }catch(Exception e){
                ex = e;
            }
            assertNull(ex);
            Map<String, IECSNode> nodes = ecs.getNodes();
            for(IECSNode node : nodes.values())
            {
                assertEquals(ECSNodeMessage.ECSNodeFlag.SHUT_DOWN, node.getFlag());
            }
     }
}