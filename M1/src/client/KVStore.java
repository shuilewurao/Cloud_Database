package client;

import app_kvClient.IKVClient;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;
import shared.messages.TextMessage;
import shared.messages.KVConvertMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class KVStore implements KVCommInterface {
    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */

    private Logger logger = Logger.getRootLogger();
    private Set<IKVClient> listeners;
    private boolean running;
    private static final String DELIMITER = "+";

    private Socket clientSocket;
    private OutputStream output;
    private InputStream input;

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

    private String address;
    private int port;

    public KVStore(String address, int port) { // throws UnknownHostException, IOException {

        this.address = address;
        this.port = port;

        setRunning(true);
    }

    @Override
    public void connect() throws UnknownHostException, IOException {
        this.clientSocket = new Socket(this.address, this.port);
        this.listeners = new HashSet<IKVClient>();
        logger.info("Connection established");
        this.output = clientSocket.getOutputStream();
        this.input = clientSocket.getInputStream();
        TextMessage reply = receiveMessage();
        logger.info(reply.getMsg());
    }

    @Override
    public void disconnect() {
        logger.info("try to close connection ...");

        try {
            tearDownConnection();
            for (IKVClient listener : listeners) {
                listener.handleStatus(IKVClient.SocketStatus.DISCONNECTED);
            }
        } catch (IOException ioe) {
            logger.error("Unable to close connection!");
        }
    }

    private void tearDownConnection() throws IOException {
        setRunning(false);
        logger.info("tearing down the connection ...");
        if (clientSocket != null) {
            input.close();
            output.close();
            clientSocket.close();
            clientSocket = null;
            logger.info("connection closed!");
        }
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        if (checkKeyValue(key, value)) {

            /*
                TextMessage.getMsh = String

                Marshall Message/Message to be send:

                CMD + DELIMITER + KEY + DELIMITER + VALUE (Optional)

                CMD TYPE:

                PUT + DELIMITER + KEY + DELIMITER + VALUE // key dne in server -> insert
                PUT + DELIMITER + KEY + DELIMITER + VALUE // key exists in server -> update

                DELETE + DELIMITER + KEY
                GET + DELIMITER + KEY
             */

            String msg;

            if (value.equals("null")) {
                logger.debug("KVStore delete");
                msg = "PUT" + DELIMITER + key + DELIMITER + "null";
            } else {
                msg = "PUT" + DELIMITER + key + DELIMITER + value;
            }

            TextMessage msg_send = new TextMessage(msg);

            sendMessage(msg_send);

            TextMessage msg_receive = receiveMessage();

            logger.debug("From server: " + msg_receive.getMsg());

            /*
                return message should be a StatusType string
                to be converted to KVMessage
             */

            msg = msg_receive.getMsg().trim();
            logger.debug("KVMsg from KVStore.get: " + msg);

            if(value.equals("") || value.equals("null")){
                return new KVConvertMessage(key, "null", msg);
            }else{
                return new KVConvertMessage(key, value, msg);
            }
        } else {
            return new KVConvertMessage(key, value, "PUT_ERROR");
        }

    }

    @Override
    public KVMessage get(String key) throws Exception {
        if (checkKeyValue(key, "")) {

            String msg = "GET" + DELIMITER + key + DELIMITER;
            logger.info("GET msg to send: " + msg);

            TextMessage msg_send = new TextMessage(msg);

            sendMessage(msg_send);

            TextMessage msg_receive = receiveMessage();

            msg = msg_receive.getMsg().trim();

            /*
                return msg should be
                KVMessage + DELIMITER + KEY + DELIMITER + VALUE (Optional)
             */

            String[] tokens = msg.split("\\" + DELIMITER);
            logger.debug("KVMsg from KVStore.get: " + msg);

            String value = null;

            if (tokens.length == 3) {
                value = tokens[2];
            }

            return new KVConvertMessage(tokens[1], value, tokens[0]);

        } else {
            return new KVConvertMessage(key, null, "GET_ERROR");
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
        logger.info("Send message:\t '" + msg.getMsg() + "'");
    }

    private TextMessage receiveMessage() throws IOException {

        int index = 0;
        byte[] msgBytes = null, tmp = null;
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
        logger.info("Receive message:\t '" + msg.getMsg().trim() + "'");
        return msg;
    }

    private boolean checkKeyValue(String key, String value) {

        if (key.length() > BUFFER_SIZE) {
            logger.error("KEY size exceeds limit: " + BUFFER_SIZE);
            return false ;
        }

        if (value.length() > DROP_SIZE) {
            logger.error("VALUE size exceeds limit: " + DROP_SIZE);
            return false ;
        }

        Pattern p = Pattern.compile("[^A-Za-z0-9]");
        Matcher m = p.matcher(key);

        if (m.find()) {
            logger.error("There is a special character in Key ");
            return false;
        }

        if (!value.isEmpty()) {
            m = p.matcher(value);

            if (m.find()) {
                logger.error("There is a special character in Value ");
                return false;
            }
        }

        logger.info("Valid Key and Value ");
        return true;
    }
}
