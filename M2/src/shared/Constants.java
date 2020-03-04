package shared;

public interface Constants {
    public static final String DELIMITER = "+";
    public static final String HASH_DELIMITER = ":";

    public static final String ESCAPER = "-";
    public static final String DELIM = ESCAPER + ",";
    public static final String ESCAPED_ESCAPER = ESCAPER + "d";

    public static final String DB_DIR = "./MyStore";

    public static final int BUFFER_SIZE = 1024;
    public static final int DROP_SIZE = 1024 * BUFFER_SIZE;

}
