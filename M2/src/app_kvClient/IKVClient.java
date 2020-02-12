package app_kvClient;

import client.KVCommInterface;
import shared.messages.TextMessage;

public interface IKVClient {
    /**
     * Creates a new connection to hostname:port
     *
     * @throws Exception when a connection to the server can not be established
     */
    public void newConnection(String hostname, int port) throws Exception;

    /**
     * Get the current instance of the Store object
     *
     * @return instance of KVCommInterface
     */
    public KVCommInterface getStore();

    // ADDED FROM CLIENTSOCKETLISTERNER.JAVA
    public enum SocketStatus {CONNECTED, DISCONNECTED, CONNECTION_LOST};

    public void handleStatus(IKVClient.SocketStatus status);
}
