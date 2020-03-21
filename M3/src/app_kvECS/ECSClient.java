package app_kvECS;

import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

public class ECSClient implements IECSClient {

    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ECSClient >> ";
    private static final String PROMPTIN = "ECSClient << ";

    private String config_file_path;
    private boolean stop = false;

    private ECS client = null;

    public ECSClient(String config_file_path) {
        this.config_file_path = config_file_path;
    }

    public void run() throws Exception {

        client = new ECS(config_file_path);

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

    private void handleCommand(String cmdLine) throws Exception {
        String cmdLineTrim = cmdLine.trim();
        logger.debug("[ECSClient] User input: " + cmdLineTrim);
        String[] tokens = cmdLineTrim.split("\\s+");

        switch (tokens[0]) {
            case "shutdown":
                if (client != null) {
                    if (shutdown()) {
                        stop = true;
                        logger.info("[ECSClient] Application exit!");
                        System.out.println(PROMPT + "Application exit!");
                        System.exit(0);
                    } else {
                        logger.error("[ECSClient] Cannot quit!");
                        System.out.println(PROMPT + "Application exit!");
                        System.exit(1);
                    }
                }

                logger.info("[ECSClient] Application exit!");
                System.out.println(PROMPT + "Application exit!");
                System.exit(0);

            case "init":
                if (tokens.length == 4) {
                    try {
                        int numberOfNodes = Integer.parseInt(tokens[1]);
                        int cacheSize = Integer.parseInt(tokens[2]);
                        String replacementStrategy = tokens[3];

                        addNodes(numberOfNodes, replacementStrategy, cacheSize);

                    } catch (NumberFormatException nfe) {
                        printError("No valid address. Port must be a number!");
                        logger.info("[ECSClient] Unable to parse argument <port>", nfe);
                    } catch (Exception e) {
                        printError("Could not establish connection!");
                        logger.warn("[ECSClient] Could not establish connection!", e);
                        e.printStackTrace();
                    }
                } else {
                    logger.error("[ECSClient] user input error for command \"init\"!");
                    printError("Invalid number of parameters!");
                }
                break;
            case "add":
                if (tokens.length == 3) {
                    int cacheSize = Integer.parseInt(tokens[1]);
                    String replacementStrategy = tokens[2];

                    try {
                        addNode(replacementStrategy, cacheSize, true);

                    } catch (NumberFormatException nfe) {
                        printError("No valid address. Port must be a number!");
                        logger.info("[ECSClient] Unable to parse argument <port>", nfe);
                    } catch (Exception e) {
                        printError("Could not establish connection!");
                        logger.warn("[ECSClient] Could not establish connection!", e);
                        e.printStackTrace();
                    }
                } else {
                    logger.error("[ECSClient] user input error for command \"add\"!");
                    printError("Invalid number of parameters!");
                }


                break;
            case "remove":
                if (tokens.length >= 2) {


                    try {
                        String[] indexArr = Arrays.stream(tokens, 1, tokens.length).toArray(String[]::new);

                        Collection<String> index = Arrays.asList(indexArr);
                        removeNodes(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else {
                    logger.error("[ECSClient] user input error for command \"remove\"!");
                    printError("Invalid number of parameters!");
                }


                break;
            case "start":
                if (tokens.length == 1) {
                    if (client != null) {
                        try {
                            start();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    } else {
                        logger.error("[ECSClient] Not connected!");
                        printError("Not connected!");
                    }
                } else {
                    logger.error("[ECSClient]] user input error for command \"start\"!");
                    printError("Invalid number of parameters!");
                }

                break;
            case "stop":
                if (tokens.length == 1) {
                    if (client != null) {
                        try {

                            stop();

                        } catch (Exception e) {
                            logger.error("[ECSClient] GET_ERROR + exception: " + e);
                            e.printStackTrace();
                        }
                    } else {
                        logger.error("[ECSClient] Not connected!");
                        printError("Not connected!");
                    }
                } else {
                    logger.error("[ECSClient]] user input error for command \"stop\"!");
                    printError("Invalid number of parameters!");
                }

                break;
            case "logLevel":
                if (tokens.length == 2) {
                    String level = setLevel(tokens[1]);
                    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                        logger.error("[ECSClient] No valid log level!");
                        printError("No valid log level!");
                        printPossibleLogLevels();
                    } else {
                        System.out.println(PROMPT +
                                "Log level changed to level " + level);
                    }
                } else {
                    logger.error("[ECSClient] Invalid number of parameters!");
                    printError("Invalid number of parameters!");
                }

                break;
            case "help":

                printHelp();
                break;
            default:
                printError("Unknown command!");
                printHelp();
                break;
        }

    }

    private void printHelp() {

        String sb = PROMPT + "ECSClient HELP (Usage):\n" +
                PROMPT + "use \"help\" to see this list.\n" +
                PROMPT +
                "::::::::::::::::::::::::::::::::" +
                "::::::::::::::::::::::::::::::::\n" +
                PROMPT + "init <numberOfServers> <cacheSize> <replacementStrategy>\n" +
                "\t\t\t Initiates the storage service with user specified number of servers, cache size, and replacement strategy. \n" +
                PROMPT + "add <cacheSize> <replacementStrategy>\n" +
                "\t\t\t Add a new KVServer with the specified cache size and replacement strategy to the storage service. \n" +
                PROMPT + "remove <indexOfServer>\n" +
                "\t\t\t Remove the specified server from the storage service. \n" +
                PROMPT + "start\n" +
                "\t\t\t Starts the service by starting all participating KVServers. \n" +
                PROMPT + "stop\n" +
                "\t\t\t Stops the service; all participating KVServers are stopped for processing client requests. \n" +
                PROMPT + "logLevel\n" +
                "\t\t\t changes the logLevel\n" +
                "\t\t\t ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n" +
                PROMPT + "shutdown" +
                "\t\t\t exits the program";
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

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    @Override
    public boolean start() {
        try {
            return client.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean stop() {
        try {
            return client.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public boolean shutdown() throws Exception {

        return client.shutdown();
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize, boolean isSole) {
        //client.addNodes(1, cacheStrategy, cacheSize);
        client.addNode(cacheStrategy, cacheSize, true);
        return null;
    }

    @Override
    public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize) throws IOException {
        return client.addNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> setupNodes(Collection<ECSNode> nodes) {
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        if (!client.isAllValidNames(nodeNames)) {

            return false;
        }

        return client.removeNodes(nodeNames);
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return client.getNodes();
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return client.getNodeByKey(Key);
    }

    public static void main(String[] args) {
        // TODO
        try {
            new LogSetup("logs/ECSClient.log", Level.ALL);
            if (args.length != 1) {
                logger.error("[ECSClient] Error! Invalid number of arguments!");
                logger.info("[ECSClient] Usage: ECS <ecs.config>!");
            } else {
                String config_file_path = args[0];
                File tmpFile = new File(config_file_path);
                if (tmpFile.exists()) {

                    ECSClient app = new ECSClient(config_file_path);

                    app.run();

                } else {
                    logger.error("[ECSClient] Error! File does not exist!" + config_file_path);
                    logger.info("[ECSClient] Program exits...");
                    System.exit(1);
                }
            }
        } catch (IOException e) {
            logger.error("[ECSClient] Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException nfe) { //TODO

            logger.error("[ECSClient] Error! Invalid argument format!");
            logger.info("[ECSClient] Usage: ECS <ecs.config>!");
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
