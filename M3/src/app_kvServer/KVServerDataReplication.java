package app_kvServer;

import app_kvClient.IKVClient;
import ecs.ECSNode;
import org.apache.log4j.Logger;
import shared.Constants;
import shared.messages.TextMessage;

import java.io.*;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class KVServerDataReplication {

    private Logger logger = Logger.getRootLogger();

    private String name;
    private String host;
    private int port;
    private boolean running;
    private Socket clientSocket;
    private Set<IKVClient> listeners;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = Constants.BUFFER_SIZE;
    private static final int DROP_SIZE = Constants.DROP_SIZE;

    private String prompt = "[KVServerDR] ";


    public KVServerDataReplication(ECSNode n) {
        assert n != null;
        this.name = n.getNodeName();
        this.host = n.getNodeHost();
        this.port = n.getNodePort();
    }

    public String getServerName() {
        return name;
    }

    public void dataReplication(String cmd, String k, String v) throws IOException {
        assert !cmd.equals("PUT");

        String msg;

        msg = "PUT_REPLICATE" + Constants.DELIMITER + k + Constants.DELIMITER + v;

        sendMessage(new TextMessage(msg));

        String msg_receive = receiveMessage().getMsg().trim();

        String[] tokens = msg_receive.split("\\" + Constants.DELIMITER);

        if (tokens[0].equals("PUT_SUCCESS") || tokens[0].equals("PUT_UPDATE") || tokens[0].equals("DELETE_SUCCESS"))
            logger.warn(prompt + cmd + " " + k + " " + v + " in " + this.name + " failed!");

    }

    public void connect() throws IOException {
        this.clientSocket = new Socket(this.host, this.port);
        this.listeners = new HashSet<>();
        logger.info("[KVStore] Connection established");
        this.output = clientSocket.getOutputStream();
        this.input = clientSocket.getInputStream();
        TextMessage reply = receiveMessage();
        setRunning(true);
        logger.info("[KVStore] " + reply.getMsg());
    }


    public void disconnect() {
        logger.info("[KVStore] try to close connection ...");

        try {
            tearDownConnection();
            for (IKVClient listener : listeners) {
                listener.handleStatus(IKVClient.SocketStatus.DISCONNECTED);
            }
        } catch (IOException ioe) {
            logger.error("[KVStore] Unable to close connection!");
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean run) {
        running = run;
    }

    public void addListener(IKVClient listener) {
        listeners.add(listener);
    }

    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("[KVStore] tearing down the connection ...");
        if (clientSocket != null) {
            input.close();
            output.close();
            clientSocket.close();
            clientSocket = null;
            logger.info("[KVStore] connection closed!");
        }
    }

    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info(prompt + "Sending: " + msg.getMsg());
    }

    private TextMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while (read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                if (msgBytes == null) {
                    tmp = new byte[BUFFER_SIZE];
                    System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
                } else {
                    tmp = new byte[msgBytes.length + BUFFER_SIZE];
                    System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
                    System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
                            BUFFER_SIZE);
                }

                msgBytes = tmp;
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and numbers */
            if ((read > 31 && read < 127)) {
                bufferBytes[index] = read;
                index++;
            }

            /* stop reading is DROP_SIZE is reached */
            if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        if (msgBytes == null) {
            tmp = new byte[index];
            System.arraycopy(bufferBytes, 0, tmp, 0, index);
        } else {
            tmp = new byte[msgBytes.length + index];
            System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
        }

        msgBytes = tmp;

        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);
        logger.debug("[KVStore] Received message from server: " + msg.getMsg().trim());
        return msg;
    }

}
