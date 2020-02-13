package app_kvServer.DataObjects;

import app_kvServer.IKVServer.ServerStateType;

import java.io.Serializable;

public class MetaData implements Serializable{
    private String name;
    private String host;
    private int port;
    private String startHash;
    private String endHash;

    private boolean writeLocked;
    private ServerStateType serverState;

    public MetaData(String name, String host, int port,
                    String startHash, String endHash) {
        this.name = name;
        this.host = host;
        this.port = port;
        this.startHash = startHash;
        this.endHash = endHash;
        this.port = port;
        writeLocked = false;
        serverState = ServerStateType.STOPPED;
    }

    public String getName(){
        return name;
    }

    public String getHost(){
        return host;
    }

    public int getPort(){
        return port;
    }

    public String[] getHashRange(){
        String[] hashes = new String[2];
        hashes[0] = startHash;
        hashes[1] = endHash;
        return hashes;
    }

    public ServerStateType getServerStateType() {
        return serverState;
    }

    public boolean isWriteLocked() {
        return writeLocked;
    }

    public void setServerStateType(ServerStateType state){
        serverState = state;
    }

    public void setWriteLocked(boolean locked){
        writeLocked = locked;
    }
//
//    public void setHost(String host){
//        this.host = host;
//    }
}
