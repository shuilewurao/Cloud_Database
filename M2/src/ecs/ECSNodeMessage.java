package ecs;

public interface ECSNodeMessage {
    enum ECSNodeFlag {
        INIT,
        STOP,              /* Node has stopped */
        START,             /* Node has started */
        STATE_CHANGE,      /* Node state has changed*/
        KV_TRANSFER,       /* Data transfer occurred */
        SHUT_DOWN,         /* Node has shutdown*/
        UPDATE,            /* Node has updated */
        TRANSFER_FINISH    /* Data transfer operation finished */
    }
}
