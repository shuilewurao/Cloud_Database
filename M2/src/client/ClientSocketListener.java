package client;

import shared.messages.TextMessage;

public interface ClientSocketListener {

    public enum SocketStatus {CONNECTED, DISCONNECTED, CONNECTION_LOST}

    public void handleNewMessage(TextMessage msg);

    public void handleStatus(SocketStatus status);
}
