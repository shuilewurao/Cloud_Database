package app_kvServer.Database;

import shared.Constants;

import java.util.Arrays;

public class WALEntry {
    private long LSN;
    private String K;
    private String V;
    private String type;
    private long[] timestamp;
    int client_port;
    long msg_ts;

    public WALEntry(long seq, String key, String value, String cmd, int clientPort, long msg_ts, long[] ts){
        LSN=seq;
        K=key;
        V=value;
        type=cmd;
        timestamp=ts;
        client_port= clientPort;
    }

    public String getEntry(){
        return LSN+Constants.DELIM +
                K +Constants.DELIM +
                V + Constants.DELIM +
                encodeValue(type) + Constants.DELIM +
                client_port + Constants.DELIM +
                msg_ts + Constants.DELIM +
                Arrays.toString(timestamp) + "\r\n";
    }

    private String encodeValue(String value) {
        return value.replaceAll("\r", "\\\\r")
                .replaceAll("\n", "\\\\n")
                .replaceAll(Constants.ESCAPER, Constants.ESCAPED_ESCAPER);
    }

    private String decodeValue(String value) {
        return value.replaceAll("\\\\r", "\r")
                .replaceAll("\\\\n", "\n")
                .replaceAll(Constants.ESCAPED_ESCAPER, Constants.ESCAPER);
    }

}
