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


public class HashringTest extends TestCase {
	private ECSHashRing hr_test;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    private  int numGetClients = 5;

    public void setUp() {
    	try{
     		hr_test = new ECSHashRing();
    	}catch(Exception e){
    		//System.out.println("ECS Test error "+e);
    	}
    }

    public void tearDown() {
    	try{
    	}catch(Exception e){
    		//System.out.println("ECS Test error "+e);
    	}
    }

    public void test_addNodes(){
    	Exception ex = null;
    	try{
	    	hr_test.addNode(new ECSNode("1", "localhost", 50001));
	    	hr_test.addNode(new ECSNode("2", "localhost", 50002));
	    	hr_test.addNode(new ECSNode("3", "localhost", 50003));
    	}catch(Exception e)
    	{
    		ex = e;
    	}
    	assertNull(ex);
    }

    public void test_getNodes(){
    	Exception ex = null;
    	try{
    		ECSNode temp = new ECSNode("4", "localhost", 50004);
	    	hr_test.getNodeByName("4");
	    	hr_test.getNodeByHash(temp.getNodeHash());
    	}catch(Exception e)
    	{
    		ex = e;
    	}
    	assertNull(ex);
    }

    public void test_removeNodes(){
    	Exception ex = null;
    	try{
	    	hr_test.removeNode(hr_test.getNodeByName("4"));
    	}catch(Exception e)
    	{
    		ex = e;
    	}
    	assertNull(ex);
    }
}