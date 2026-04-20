package client;

import model.NodeInfo;

import java.util.List;
import java.util.Arrays;

public class MainClient {
    public static void main(String[] args) {

        List<NodeInfo> nodes = Arrays.asList(
                new NodeInfo(1, "localhost", 50051),
                new NodeInfo(2, "localhost", 50052),
                new NodeInfo(3, "localhost", 50053)
        );

        TaskClient client = new TaskClient(nodes);

        client.sendTask("T1", "hello world");
    }
}
