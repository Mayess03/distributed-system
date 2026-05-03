package client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import proto.TaskServiceGrpc;
import proto.TaskRequest;
import proto.TaskResponse;
import model.NodeInfo;

import java.util.List;
import java.util.Random;

public class TaskClientGrpc {

    private List<NodeInfo> nodes;

    public TaskClientGrpc(List<NodeInfo> nodes) {
        this.nodes = nodes;
    }

    public void sendTask(String taskId, String payload) {

        NodeInfo selected = nodes.get(new Random().nextInt(nodes.size()));

        System.out.println("\n📨 Sending to node " + selected.id);

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(selected.host, selected.port)
                .usePlaintext()
                .build();

        TaskServiceGrpc.TaskServiceBlockingStub stub =
                TaskServiceGrpc.newBlockingStub(channel);

        TaskRequest request = TaskRequest.newBuilder()
                .setTaskId(taskId)
                .setPayload(payload)
                .setAlreadyConsensusDone(false)
                .build();

        TaskResponse response = stub.submitTask(request);

        System.out.println("📥 Response: " + response.getResult());

        channel.shutdown();
    }

    // 🔥 utilisé pour redirection interne
    public TaskResponse sendToLeader(TaskRequest request) {

        NodeInfo leader = nodes.stream()
                .min((a, b) -> Integer.compare(a.id, b.id))
                .orElse(null);

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(leader.host, leader.port)
                .usePlaintext()
                .build();

        TaskServiceGrpc.TaskServiceBlockingStub stub =
                TaskServiceGrpc.newBlockingStub(channel);

        TaskResponse response = stub.submitTask(request);

        channel.shutdown();

        return response;
    }
    public TaskResponse sendToNode(TaskRequest request, NodeInfo node) {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(node.host, node.port)
                .usePlaintext()
                .build();

        TaskServiceGrpc.TaskServiceBlockingStub stub =
                TaskServiceGrpc.newBlockingStub(channel);

        TaskResponse response = stub.submitTask(request);

        channel.shutdown();

        return response;
    }
}