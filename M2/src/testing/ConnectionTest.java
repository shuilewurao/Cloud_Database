package testing;

import java.net.UnknownHostException;

import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

    public void testConnectionSuccess() {

        Exception ex = null;

        KVStore kvClient = new KVStore("localhost", 51000);
        System.out.println("new KVStore");

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
            System.out.println("exception");
        }

        assertNull(ex);
    }


    public void testUnknownHost() {
        Exception ex = null;
        KVStore kvClient = new KVStore("unknown", 51000);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof UnknownHostException);
    }


    public void testIllegalPort() {
        Exception ex = null;
        KVStore kvClient = new KVStore("localhost", 123456789);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex instanceof IllegalArgumentException);
    }


}

