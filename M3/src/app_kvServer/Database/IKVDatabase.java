package app_kvServer.Database;

import shared.messages.KVMessage.StatusType;

public interface IKVDatabase {

    void clearStorage();

    String getKV(String K) throws Exception;

    StatusType putKV(String K, String V) throws Exception;

    boolean inStorage(String K);

    // String getFileName();

}
