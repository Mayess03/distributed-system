package server;

import model.NodeInfo;
import java.util.Arrays;
import java.util.List;

public class MainNode3 {
    public static void main(String[] args) throws Exception {

        List<NodeInfo> nodes = Arrays.asList(
                new NodeInfo(1, "localhost", 50051),
                new NodeInfo(2, "localhost", 50052),
                new NodeInfo(3, "localhost", 50053)
        );

        new TaskServer(3, 50053, nodes).start();
    }
}