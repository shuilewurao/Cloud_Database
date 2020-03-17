package shared.messages;

public class KVConvertMessage implements KVMessage {
    private String key;
    private String value;
    private StatusType status;

    public KVConvertMessage(String key, String value, String status) {
        this.key = key;
        this.value = value;
        this.status = null;

        String trimmedStatus = status.trim();

//            logger.debug("Parsing trimmed status: " + trimmedStatus);

        switch (trimmedStatus) {
            case "GET":
                this.status = StatusType.GET;
                break;
            case "GET_ERROR":
                this.status = StatusType.GET_ERROR;
                break;
            case "GET_SUCCESS":
                this.status = StatusType.GET_SUCCESS;
                break;
            case "PUT":
                this.status = StatusType.PUT;
                break;
            case "PUT_SUCCESS":
                this.status = StatusType.PUT_SUCCESS;
                break;
            case "PUT_UPDATE":
                this.status = StatusType.PUT_UPDATE;
                break;
            case "PUT_ERROR":
                this.status = StatusType.PUT_ERROR;
                break;
            case "DELETE_SUCCESS":
                this.status = StatusType.DELETE_SUCCESS;
                break;
            case "DELETE_ERROR":
                this.status = StatusType.DELETE_ERROR;
                break;
            case "SERVER_STOPPED":
                this.status = StatusType.SERVER_STOPPED;
                break;
            case "SERVER_WRITE_LOCK":
                this.status = StatusType.SERVER_WRITE_LOCK;
                break;
            case "SERVER_NOT_RESPONSIBLE":
                this.status = StatusType.SERVER_NOT_RESPONSIBLE;
                break;
            default:
                System.out.println("[KVConvertMessage] Default case for parsing status in KVConvertMessage");
        }
    }

    public KVConvertMessage(String key, String value, StatusType status) {
        this.key = key;
        this.value = value;
        this.status = status;
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    @Override
    public String getKey() {
        return key;
    }

    ;

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    @Override
    public String getValue() {
        return value;
    }

    ;

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    @Override
    public StatusType getStatus() {
        return status;
    }
}
