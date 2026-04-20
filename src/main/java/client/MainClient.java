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

        // 🔥 TEST 1 : même tâche 2 fois
        System.out.println("=== TEST 1: SAME TASK ===");
        client.sendTask("T1", "hello world");
        client.sendTask("T1", "hello world");

        // 🔥 TEST 2 : tâches différentes
        System.out.println("\n=== TEST 2: DIFFERENT TASKS ===");
        client.sendTask("T2", "data A");
        client.sendTask("T3", "data B");

        // 🔥 TEST 3 : concurrence
        System.out.println("\n=== TEST 3: CONCURRENT TASK ===");

        Thread t1 = new Thread(() -> client.sendTask("T4", "parallel"));
        Thread t2 = new Thread(() -> client.sendTask("T4", "parallel"));

        t1.start();
        t2.start();
    }
}