package app_kvClient;

import client.*;
import logger.LogSetup;
import org.apache.log4j.Logger;

import org.apache.log4j.Level;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "KVClient >> ";
    private static final String PROMPTIN = "KVClient << ";
    private BufferedReader stdin;
    private KVStore client = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public void run() {
        while (!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPTIN);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String cmdLineTrim = cmdLine.trim();
        logger.info("Trimmed: " + cmdLineTrim);
        String[] tokens = cmdLineTrim.split("\\s+");

        if (tokens[0].equals("quit")) {
            stop = true;
            if (client != null) {
                disconnect();
            }
            logger.info(PROMPT + "Application exit!");
            System.out.println(PROMPT + "Application exit!");

        } else if (tokens[0].equals("connect")) {
            if (tokens.length == 3) {
                try {
                    serverAddress = tokens[1];
                    serverPort = Integer.parseInt(tokens[2]);
                    newConnection(serverAddress, serverPort);
                } catch (NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                } catch (Exception e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                }
            } else {
                logger.error("Invalid number of parameters!");
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("put")) {
            if (tokens.length >= 2) {
                if (client != null && client.isRunning()) {
                    if (tokens.length == 3) {
                        try {
                            KVMessage msg_receive = client.put(tokens[1], tokens[2]);

                            KVMessage.StatusType status = msg_receive.getStatus();

                            System.out.println("Returned status: " + getKVStatus(status));

                            if (getKVStatus(status) == "PUT_SUCCESS") {
                                logger.info("PUT_SUCCESS" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                                System.out.println("PUT_SUCCESS" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                            } else if (getKVStatus(status) == "PUT_UPDATE") {
                                logger.info("PUT_UPDATE" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                                System.out.println("PUT_UPDATE" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                            } else {
                                logger.error("PUT_ERROR + NULL status");
                                System.out.println("PUT_ERROR + NULL status");
                            }

                        } catch (Exception e) {
                            logger.error("PUT_ERROR + NULL status");
                            System.out.println("PUT_ERROR + exception");
                        }
                    } else {
                        // deletes
                        try {
                            KVMessage msg_receive = client.put(tokens[1], "");

                            KVMessage.StatusType status = msg_receive.getStatus();

                            if (getKVStatus(status) == "DELETE_SUCCESS") {
                                logger.info("DELETE_SUCCESS" + " for key = " + tokens[1]);
                                System.out.println("DELETE_SUCCESS" + " for key = " + tokens[1]);
                            } else {
                                logger.error("DELETE_ERROR");
                                System.out.println("DELETE_ERROR");
                            }
                        } catch (Exception e) {
                            logger.error("DELETE_ERROR");
                            System.out.println("DELETE_ERROR");
                        }
                    }

                } else {
                    logger.error("Not connected!");
                    printError("Not connected!");
                }
            } else {
                logger.error("Not connected!");
                printError("No key and value passed!");
            }

        } else if (tokens[0].equals("get")) {
            if (tokens.length == 2) {
                if (client != null && client.isRunning()) {
                    try {

                        KVMessage msg_receive = client.get(tokens[1]);

                        String key = msg_receive.getKey();
                        String value = msg_receive.getValue();
                        KVMessage.StatusType status = msg_receive.getStatus();

                        if (getKVStatus(status) == "GET_SUCCESS") {
                            logger.info("GET_SUCCESS" + " for key = " + key + ", " + "value = " + value);
                            System.out.println("GET_SUCCESS" + " for key = " + key + ", " + "value = " + value);
                        } else {
                            logger.error("GET_ERROR + NULL status");
                            System.out.println("GET_ERROR + NULL status");
                        }

                    } catch (Exception e) {
                        logger.error("GET_ERROR + exception");
                    }
                } else {
                    logger.error("Not connected!");
                    printError("Not connected!");
                }
            } else {
                logger.error("No key passed!");
                printError("No key passed!");
            }

        } else if (tokens[0].equals("disconnect")) {
            disconnect();

        } else if (tokens[0].equals("logLevel")) {
            if (tokens.length == 2) {
                String level = setLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    logger.error("No valid log level!");
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else {
                logger.error("Invalid number of parameters!");
                printError("Invalid number of parameters!");
            }

        } else if (tokens[0].equals("help")) {
            printHelp();
        } else {
            printError("Unknown command");
            printHelp();
        }
    }

    @Override
    public void newConnection(String address, int port) throws Exception {
        client = new KVStore(address, port);
        client.connect();
        client.addListener(this);
        logger.info(PROMPT + "Connection established \n");
        System.out.print(PROMPT + "Connection established \n");
    }

    @Override
    public KVCommInterface getStore() {
        // TODO Auto-generated method stub
        return this.client;
    }

    private void disconnect() {
        if (client != null) {
            client.disconnect();
            client = null;
        } else {
            printError("Not connected");
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("KVCLIENT HELP (Usage):\n");
        sb.append(PROMPT).append("use \"help\" to see this list.\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t\t disconnects from the server \n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t inserts a key-value pair into the storage. \n");
        sb.append("\t\t\t\t\t\t\t\t\t updates the current value if key exists. \n");
        sb.append("\t\t\t\t\t\t\t\t\t deletes the entry for the key if <value> is null. \n");
        sb.append(PROMPT).append("get <key> ");
        sb.append("\t\t\t\t retrieves the value for the given key. \n");
        sb.append(PROMPT).append("logLevel");
        sb.append("\t\t\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t\t\t exits the program");
        System.out.println(sb.toString());
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    private String setLevel(String levelString) {

        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private String getKVStatus(StatusType status) {
        if (status == StatusType.GET_ERROR) {
            return "GET_ERROR";
        } else if (status == StatusType.GET_SUCCESS) {
            return "GET_SUCCESS";
        } else if (status == StatusType.PUT_SUCCESS) {
            return "PUT_SUCCESS";
        } else if (status == StatusType.PUT_UPDATE) {
            return "PUT_UPDATE";
        } else if (status == StatusType.PUT_ERROR) {
            return "PUT_ERROR";
        } else if (status == StatusType.DELETE_SUCCESS) {
            return "DELETE_SUCCESS";
        } else if (status == StatusType.DELETE_ERROR) {
            return "DELETE_ERROR";
        } else {
            return "";
        }
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if (status == SocketStatus.CONNECTED) {

        } else if (status == SocketStatus.DISCONNECTED) {
            System.out.print(PROMPT);
            logger.error("Connection terminated: "
                    + serverAddress + " / " + serverPort);
            System.out.println("Connection terminated: "
                    + serverAddress + " / " + serverPort);

        } else if (status == SocketStatus.CONNECTION_LOST) {
            logger.error("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.println("Connection lost: "
                    + serverAddress + " / " + serverPort);
            System.out.print(PROMPT);
        }

    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    /**
     * Main entry point for the echo server application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient app = new KVClient();
            app.run();
        } catch (IOException e) {
            logger.error("Error! Unable to initialize logger!");
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
