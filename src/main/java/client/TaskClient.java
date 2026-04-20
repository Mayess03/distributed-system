package client;

import consensus.LeaderElection;
import model.NodeInfo;
import mutex.DistributedLock;

import java.util.List;
import java.util.Random;

public class TaskClient {

    private List<NodeInfo> nodes;

    public TaskClient(List<NodeInfo> nodes) {
        this.nodes = nodes;
    }

    public void sendTask(String taskId, String payload) {

        NodeInfo selected = nodes.get(new Random().nextInt(nodes.size()));
        NodeInfo leader = LeaderElection.electLeader(nodes);

        System.out.println("\n--- NEW REQUEST ---");
        System.out.println("Selected node: " + selected.id);
        System.out.println("Leader node: " + leader.id);

        if (selected.id != leader.id) {
            System.out.println("Redirecting to leader...");
            selected = leader;
        }

        // 🔥 MUTEX CHECK
        boolean locked = DistributedLock.acquire(taskId);

        if (!locked) {
            System.out.println("Task " + taskId + " already executed or locked ❌");
            return;
        }

        // 🔥 EXECUTION
        System.out.println("Task executed by node " + selected.id + " with payload: " + payload);

        // 🔥 RELEASE + MARK EXECUTED
        DistributedLock.release(taskId);
    }
}