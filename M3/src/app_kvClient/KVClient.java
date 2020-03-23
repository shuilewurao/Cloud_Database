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
    private KVStore client = null;
    private boolean stop = false;

    private String serverAddress;
    private int serverPort;

    public void run() {
        while (!stop) {
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
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
        logger.debug("[KVClient] Trimmed: " + cmdLineTrim);
        String[] tokens = cmdLineTrim.split("\\s+");

        switch (tokens[0]) {
            case "quit":
                stop = true;
                if (client != null) {
                    disconnect();
                }
                logger.info("[KVClient] Application exit!");
                System.out.println(PROMPT + "Application exit!");

                break;
            case "connect":
                if (tokens.length == 3) {
                    try {
                        serverAddress = tokens[1];
                        serverPort = Integer.parseInt(tokens[2]);
                        newConnection(serverAddress, serverPort);
                    } catch (NumberFormatException nfe) {
                        printError("No valid address. Port must be a number!");
                        logger.info("[KVClient] Unable to parse argument <port>", nfe);
                    } catch (UnknownHostException e) {
                        printError("Unknown Host!");
                        logger.info("[KVClient] Unknown Host!", e);
                    } catch (Exception e) {
                        printError("Could not establish connection!");
                        logger.warn("[KVClient] Could not establish connection!", e);
                    }
                } else {
                    logger.error("[KVClient] Invalid number of parameters!");
                    printError("Invalid number of parameters!");
                }

                break;
            case "put":
                if (tokens.length >= 2) {
                    if (client != null && client.isRunning()) {
                        if (tokens.length == 3) {
                            try {
                                KVMessage msg_receive = client.put(tokens[1], tokens[2]);

                                if (msg_receive == null) {
                                    logger.warn("[KVClient] Connection error! Please reconnect...");
                                    break;
                                }

                                StatusType status = msg_receive.getStatus();

                                if (status == null) {
                                    logger.warn("[KVClient] NULL status returned for PUT!");
                                    break;
                                }

                                System.out.println(PROMPT + "Returned status: " + status.name());

                                if (status == StatusType.PUT_SUCCESS) {
                                    logger.info("[KVClient] PUT_SUCCESS" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                                    System.out.println(PROMPT + "PUT_SUCCESS" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                                } else if (status == StatusType.PUT_UPDATE) {
                                    logger.info("[KVClient] PUT_UPDATE" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                                    System.out.println(PROMPT + "PUT_UPDATE" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                                } else if (status == StatusType.SERVER_NOT_RESPONSIBLE) {
                                    logger.info("[KVClient] SERVER_NOT_RESPONSIBLE" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                                    System.out.println(PROMPT + "SERVER_NOT_RESPONSIBLE" + " for key = " + tokens[1] + ", " + "value = " + tokens[2]);
                                } else if (status == StatusType.SERVER_STOPPED) {
                                    logger.info("[KVClient] SERVER_STOPPED");
                                    System.out.println(PROMPT + "SERVER_STOPPED");
                                } else if (status == StatusType.SERVER_WRITE_LOCK) {
                                    logger.info("[KVClient] SERVER_WRITE_LOCK");
                                    System.out.println(PROMPT + "SERVER_WRITE_LOCK");
                                } else {
                                    logger.error("[KVClient] PUT_ERROR + NULL status");
                                }

                            } catch (Exception e) {
                                logger.error("[KVClient] PUT_ERROR: " + e);
                                e.printStackTrace();

                            }
                        } else if (tokens.length == 2) {
                            // is this a delete ?
                            try {
                                KVMessage msg_receive = client.put(tokens[1], null);

                                StatusType status = msg_receive.getStatus();

                                if (status == StatusType.DELETE_SUCCESS) {
                                    logger.info("[KVClient] DELETE_SUCCESS" + " for key = " + tokens[1]);
                                    System.out.println(PROMPT + "DELETE_SUCCESS" + " for key = " + tokens[1]);
                                } else if (status == StatusType.DELETE_ERROR) {
                                    logger.error("[KVClient] DELETE_ERROR");
                                    System.out.println(PROMPT + "DELETE_ERROR");
                                } else if (status == StatusType.SERVER_NOT_RESPONSIBLE) {
                                    logger.info("[KVClient] SERVER_NOT_RESPONSIBLE" + " for key = " + tokens[1]);
                                    System.out.println(PROMPT + "SERVER_NOT_RESPONSIBLE" + " for key = " + tokens[1]);
                                } else if (status == StatusType.SERVER_STOPPED) {
                                    logger.info("[KVClient] SERVER_STOPPED");
                                    System.out.println(PROMPT + "SERVER_STOPPED");
                                } else {
                                    logger.error("[KVClient] UNKNOWN DELETE ERROR");
                                }
                            } catch (Exception e) {
                                logger.error("[KVClient] ERROR FOR PUT: " + e);
                                System.out.println(PROMPT + "ERROR FOR PUT: " + e);
                                e.printStackTrace();
                            }
                        } else {
                            logger.error("[KVClient] Invalid number of arguments for put");
                            System.out.println(PROMPT + "Invalid number of arguments for put");
                        }

                    } else {
                        logger.error("[KVClient] Not connected!");
                        printError("Not connected!");
                    }
                } else {
                    logger.error("[KVClient] Not connected!");
                    printError("No key and value passed!");
                }

                break;
            case "get":
                if (tokens.length == 2) {
                    if (client != null && client.isRunning()) {
                        try {
                            KVMessage msg_receive = client.get(tokens[1]);

                            String key = msg_receive.getKey();
                            String value = msg_receive.getValue();
                            StatusType status = msg_receive.getStatus();

                            if (status == StatusType.GET_SUCCESS) {
                                logger.info("[KVClient] GET_SUCCESS" + " for key = " + key + ", " + "value = " + value);
                                System.out.println(PROMPT + "GET_SUCCESS" + " for key = " + key + ", " + "value = " + value);
                            } else if (status == StatusType.GET_ERROR) {
                                logger.error("[KVClient] GET_ERROR");
                                System.out.println(PROMPT + "GET_ERROR");
                            } else if (status == StatusType.SERVER_NOT_RESPONSIBLE) {
                                logger.info("[KVClient] SERVER_NOT_RESPONSIBLE" + " for key = " + key + ", " + "value = " + value);
                                System.out.println(PROMPT + "SERVER_NOT_RESPONSIBLE" + " for key = " + key + ", " + "value = " + value);
                            } else if (status == StatusType.SERVER_STOPPED) {
                                logger.info("[KVClient] SERVER_STOPPED");
                                System.out.println(PROMPT + "SERVER_STOPPED");
                            } else {
                                logger.error("[KVClient] UNKNOWN DELETE ERROR");
                            }
                        } catch (Exception e) {
                            logger.error("[KVClient] GET_ERROR + exception");
                            logger.error(e.getStackTrace());
                        }
                    } else {
                        logger.error("[KVClient] Not connected!");
                        printError("Not connected!");
                    }
                } else {
                    logger.error("[KVClient] No key passed!");
                    printError("No key passed!");
                }

                break;
            case "disconnect":
                disconnect();

                break;
            case "logLevel":
                if (tokens.length == 2) {
                    String level = setLevel(tokens[1]);
                    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                        logger.error("[KVClient] No valid log level!");
                        printError("No valid log level!");
                        printPossibleLogLevels();
                    } else {
                        System.out.println(PROMPT +
                                "Log level changed to level " + level);
                    }
                } else {
                    logger.error("[KVClient] Invalid number of parameters!");
                    printError("Invalid number of parameters!");
                }

                break;
            case "help":
                printHelp();
                break;
            default:
                printError("Unknown command");
                printHelp();
                break;
        }
    }

    @Override
    public void newConnection(String address, int port) throws Exception {
        client = new KVStore(address, port);
        client.connect();
        client.addListener(this);
        logger.info("[KVClient] Connection established \n");
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

        String sb = PROMPT + "KVCLIENT HELP (Usage):\n" +
                PROMPT + "use \"help\" to see this list.\n" +
                PROMPT +
                "::::::::::::::::::::::::::::::::" +
                "::::::::::::::::::::::::::::::::\n" +
                PROMPT + "connect <host> <port>" +
                "\t establishes a connection to a server\n" +
                PROMPT + "disconnect" +
                "\t\t\t\t disconnects from the server \n" +
                PROMPT + "put <key> <value>" +
                "\t\t inserts a key-value pair into the storage. \n" +
                "\t\t\t\t\t\t\t\t\t updates the current value if key exists. \n" +
                "\t\t\t\t\t\t\t\t\t deletes the entry for the key if <value> is null. \n" +
                PROMPT + "get <key> " +
                "\t\t\t\t retrieves the value for the given key. \n" +
                PROMPT + "logLevel" +
                "\t\t\t\t changes the logLevel \n" +
                PROMPT + "\t\t\t\t\t\t " +
                "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n" +
                PROMPT + "quit " +
                "\t\t\t\t\t exits the program";
        System.out.println(sb);
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
        return status.toString();
    }

    @Override
    public void handleStatus(SocketStatus status) {
        if (status == SocketStatus.DISCONNECTED) {
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
            logger.error("[KVClient] Error! Unable to initialize logger!");
            System.out.println(PROMPT + "Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}