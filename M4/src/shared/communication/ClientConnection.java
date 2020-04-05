package shared.communication;

import app_kvServer.IKVServer;
import app_kvServer.KVServerDataReplicationManager;
import ecs.ECSNode;
import shared.Constants;
import shared.messages.KVMessage;
import shared.messages.TextMessage;
import app_kvServer.KVServer;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.log4j.*;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    private static Logger logger = Logger.getRootLogger();

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private KVServer server;
    private KVServerDataReplicationManager dataReplicationManager;
    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    private static final String DELIMITER = "+";

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(KVServer server, Socket clientSocket) {
        this.server = server;
        this.dataReplicationManager = server.getDataReplicationManager();
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            sendMessage(new TextMessage(
                    "Connection established: "
                            + clientSocket.getLocalAddress() + " / "
                            + clientSocket.getLocalPort()));

            while (isOpen) {
                try {
                    TextMessage latestMsg = receiveMessage();

                    String msg_received = latestMsg.getMsg().trim();
                    logger.info("[ClientConnection] MSG: " + msg_received);

                    if(!server.isRunning()){
                        logger.info("Server not running");
                        disconnect();
                        return;
                    }

                    String[] tokens = msg_received.split("\\" + DELIMITER);

                    String cmd = tokens[0];
                    String key = tokens[1];

                    TextMessage msg_send;
                    logger.debug("[ClientConnection] CMD: " + cmd);
                    logger.debug("[ClientConnection] key: " + key);

                    // checks for distributed servers
                    handleClientRequest(cmd, key, tokens, msg_received);

/*
                    switch (cmd) {
                        case "PUT":

                            String value = "null";
                            if (tokens.length == 3) {
                                value = tokens[2];
                            }
                            try {
                                msg_send = new TextMessage(cmdPut(key, value));
                            } catch (Exception e) {
                                msg_send = new TextMessage("PUT_ERROR + exception");
                            }

                            break;
                        case "GET":
                            try {
                                String value_return = cmdGet(key);

                                if (value_return.equals("GET_ERROR")) {
                                    msg_send = new TextMessage("GET_ERROR");
                                } else {
                                    msg_send = new TextMessage("GET_SUCCESS" + DELIMITER + key + DELIMITER + value_return);
                                }
                            } catch (Exception e) {
                                msg_send = new TextMessage("GET_ERROR + exception");
                            }

                            break;
                        default:
                            msg_send = new TextMessage("CMD NOT RECOGNIZED: " + cmd);
                    }

                    sendMessage(msg_send);

 */

                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.error("[ClientConnection] Error! Connection lost!");
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("[ClientConnection] Error! Connection could not be established!", ioe);

        } finally {

            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("[ClientConnection] Error! Unable to tear down connection!", ioe);
            }
        }
    }

    private String cmdPut(String cmd, String key, String value, String ts, int clientPort) {
    	/*
    		return msg should be a StatusType string
    	 */

        boolean inStorage = server.inStorage(key);

        try {
            server.putKV(key, value, cmd, Long.parseLong(ts), clientPort);

//            if (cmd.equals("PUT")) {
//                boolean ret = dataReplicationManager.forward(cmd, key, value);
//            }

            if (inStorage && value.equals("")) {
                return "DELETE_SUCCESS";
            } else if (inStorage) {
                return "PUT_UPDATE";
            } else if (value.equals("")) {
                return "DELETE_ERROR";
            } else {
                return "PUT_SUCCESS";
            }


        } catch (Exception e) {
            logger.error("[ClientConnection] Error! " + e);
            e.printStackTrace();
            e.getCause();
            if (value.equals("")) {
                return "DELETE_ERROR + " + e;
            } else {
                return "PUT_ERROR + " + e;
            }
        }
    }

    private String cmdGet(String key) {
    	/*
            return msg should be
            KVMessage + KEY + DELIMITER + VALUE (Optional)
        */

        try {
            return server.getKV(key);
        } catch (Exception e) {
            return "GET_ERROR";
        }

    }

    /**
     * Method sends a TextMessage using this socket.
     *
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
        logger.info("[ClientConnection] SEND: \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg() + "'");
    }

    private TextMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

//		logger.info("First Char: " + read);
//		Check if stream is closed (read returns -1)
//		if (read == -1){
//			TextMessage msg = new TextMessage("");
//			return msg;
//		}

        while (/*read != 13  && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
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

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

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
        logger.info("[ClientConnection] RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }


    private void handleClientRequest(String cmd, String key, String[] tokens, String msg_received) throws IOException {
        TextMessage msg_send;

        // checks for distributed servers
        if (cmd.equals("Transferring_Data")) {
            try {
                String data = msg_received.split("\\" + DELIMITER, 2)[1];
                logger.debug("[ClientConnection] receiving transferred data: " + data);
                boolean result = cmdTransfer(data);
                if (result) {
                    msg_send = new TextMessage("Transferring_Data_SUCCESS");
                } else {
                    msg_send = new TextMessage("Transferring_Data_ERROR");
                }
            } catch (Exception e) {
                logger.error("[ClientConnection] Exception in data transfer");
                msg_send = new TextMessage("Transferring_Data_ERROR");
            }

        } else {
            if (this.server.getServerState() == IKVServer.ServerStateType.STOPPED && !cmd.equals("RECOVER_REPLICATE")) {
                // TODO: also needs to check if it is a ECS request

                logger.debug("[ClientConnection] current server state is:" + this.server.getServerState().name());
                msg_send = new TextMessage(KVMessage.StatusType.SERVER_STOPPED.name());

            } else if (!server.isResponsible(key, cmd)) {

                String hashRingStr = server.getHashRingStr();
                logger.debug("[ClientConnection] Server not responsible!");
                msg_send = new TextMessage(
                        KVMessage.StatusType.SERVER_NOT_RESPONSIBLE.name() + Constants.DELIMITER + hashRingStr);

            } else if (this.server.isWriteLocked() &&
                    (cmd.equals(KVMessage.StatusType.PUT.name()) ||
                            cmd.equals(KVMessage.StatusType.PUT_REPLICATE.name())
                    ) //TODO: PUT_REPLICATE
            ) {
                msg_send = new TextMessage(KVMessage.StatusType.SERVER_WRITE_LOCK.name());

            } else {
                switch (cmd) {
                    case "RECOVER_REPLICATE":
                    case "PUT_REPLICATE":
                        logger.info("[ClientConnection] processing for PUT_REPLICATE");
                    case "PUT":
                        String value = "";
                        String ts="";
                        int port=0;
                        if (tokens.length >= 3) {
                            value = tokens[2];
                            if(tokens.length > 3){
                                ts = tokens[3];
                                port=Integer.parseInt(tokens[4]);
                            }
                        }
                        try {
                            msg_send = new TextMessage(cmdPut(cmd, key, value, ts, port));
                        } catch (Exception e) {
                            logger.error("[ClientConnection] Error! " + e);
                            e.printStackTrace();
                            msg_send = new TextMessage("PUT_ERROR + exception");
                        }
                        break;
                    case "GET":
                        try {
                            String value_return = cmdGet(key);

                            if (value_return.equals("GET_ERROR")) {
                                msg_send = new TextMessage("GET_ERROR");
                            } else {
                                msg_send = new TextMessage("GET_SUCCESS" + Constants.DELIMITER + key + Constants.DELIMITER + value_return);
                            }
                        } catch (Exception e) {
                            msg_send = new TextMessage("GET_ERROR + exception");
                        }
                        break;

                    default:
                        msg_send = new TextMessage("CMD NOT RECOGNIZED: " + cmd);
                }
            }
        }
        sendMessage(msg_send);
    }

    public boolean cmdTransfer(String transferred_data) {
        // TODO
        return server.receiveTransferredData(transferred_data);

    }


    public void disconnect() {
        try {
            if (clientSocket != null) {
                this.input.close();
                this.output.close();
                clientSocket.close();
            }
        } catch (IOException e) {
            logger.error("Unable to tear down connection!", e);
        }
    }

}
