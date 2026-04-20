package server;

import mutex.DistributedLock;

public class TaskServer {
    private int nodeId;
    private int port;

    public TaskServer(int nodeId, int port) {
        this.nodeId = nodeId;
        this.port = port;
    }

    public void start() {
        System.out.println("Node " + nodeId + " started on port " + port);
        // gRPC server start ici
    }

    public String executeTask(String taskId, String payload) {

        if (!DistributedLock.acquire(taskId)) {
            return "Task already executed";
        }

        try {
            System.out.println("Node " + nodeId + " executing task " + taskId);

            Thread.sleep(1000); // simulation calcul

            return "Result from node " + nodeId + ": " + payload.toUpperCase();

        } catch (Exception e) {
            return "Error";
        } finally {
            DistributedLock.release(taskId);
        }
    }
}