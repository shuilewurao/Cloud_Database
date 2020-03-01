package shared.messages;

public interface KVMessage {

    enum StatusType {
        GET,              /* Get - request */
        GET_ERROR,        /* requested tuple (i.e. value) not found */
        GET_SUCCESS,      /* requested tuple (i.e. value) found */
        PUT,              /* Put - request */
        PUT_SUCCESS,      /* Put - request successful, tuple inserted */
        PUT_UPDATE,       /* Put - request successful, i.e. value updated */
        PUT_ERROR,        /* Put - request not successful */
        DELETE_SUCCESS,   /* Delete - request successful */
        DELETE_ERROR,     /* Delete - request successful */

        /**
         * NEW TYPE ADDED FOR M2
         */
        SERVER_STOPPED,         /* Server is stopped, no requests are processed */
        SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
        SERVER_NOT_RESPONSIBLE  /* Request not successful, server not responsible for key */
    }

    /**
     * NEW TYPE ADDED FOR M2
     */

    enum ServerStateType {
        IDLE,         /*server is idle*/
        STARTED,      /*server is started*/
        SHUT_DOWN,    /*server is shut down*/
        STOPPED       /*default server status; server is stopped*/
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    StatusType getStatus();

}


