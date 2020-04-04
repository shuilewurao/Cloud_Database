package shared;

public interface Constants {
    String DELIMITER = "+";
    String HASH_DELIMITER = ":";

    String ESCAPER = "-";
    String DELIM = ESCAPER + ",";
    String ESCAPED_ESCAPER = ESCAPER + "d";

    String DB_DIR = "./MyStore";

    int BUFFER_SIZE = 1024;
    int DROP_SIZE = 1024 * BUFFER_SIZE;

    int TIMEOUT = 2000;

}
