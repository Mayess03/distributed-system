package model;

public class NodeInfo {
    public String host;
    public int port;
    public int id;

    public NodeInfo(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }
}
