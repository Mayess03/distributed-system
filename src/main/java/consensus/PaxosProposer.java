package consensus;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import model.NodeInfo;
import proto.*;

import java.util.List;

public class PaxosProposer {

    private final List<NodeInfo> nodes;
    private final int proposerNodeId;

    public PaxosProposer(List<NodeInfo> nodes, int proposerNodeId) {
        this.nodes = nodes;
        this.proposerNodeId = proposerNodeId;
    }

    public int proposeExecutor(String taskId) {

        int proposalId = (int) (System.currentTimeMillis() % 100000) + proposerNodeId;
        int majority = nodes.size() / 2 + 1;

        int promises = 0;
        int acceptedProposalIdMax = -1;
        int chosenNode = 1;
        // ici on propose node 1 comme exécuteur par défaut
        // tu peux aussi mettre proposerNodeId si tu veux que le node contacté propose lui-même

        System.out.println("Node " + proposerNodeId + " lance Paxos pour la tâche " + taskId);
        System.out.println("Proposal ID = " + proposalId);

        // =========================
        // PHASE 1 : PREPARE
        // =========================
        for (NodeInfo node : nodes) {
            ManagedChannel channel = null;

            try {
                channel = ManagedChannelBuilder
                        .forAddress(node.host, node.port)
                        .usePlaintext()
                        .build();

                TaskServiceGrpc.TaskServiceBlockingStub stub =
                        TaskServiceGrpc.newBlockingStub(channel);

                PrepareRequest request = PrepareRequest.newBuilder()
                        .setProposalId(proposalId)
                        .setTaskId(taskId)
                        .build();

                PrepareResponse response = stub.prepare(request);

                if (response.getPromise()) {
                    promises++;

                    System.out.println("Promise reçu de node " + node.id);

                    if (response.getAcceptedProposalId() > acceptedProposalIdMax) {
                        acceptedProposalIdMax = response.getAcceptedProposalId();

                        if (response.getAcceptedNodeId() != -1) {
                            chosenNode = response.getAcceptedNodeId();
                        }
                    }
                } else {
                    System.out.println("Promise refusé par node " + node.id);
                }

            } catch (Exception e) {
                System.out.println("Node " + node.id + " indisponible pendant PREPARE");
            } finally {
                if (channel != null) {
                    channel.shutdown();
                }
            }
        }

        if (promises < majority) {
            System.out.println("Consensus échoué : pas assez de promises");
            return -1;
        }

        System.out.println("Majorité de promises obtenue : " + promises + "/" + nodes.size());

        // =========================
        // PHASE 2 : ACCEPT
        // =========================
        int accepts = 0;

        for (NodeInfo node : nodes) {
            ManagedChannel channel = null;

            try {
                channel = ManagedChannelBuilder
                        .forAddress(node.host, node.port)
                        .usePlaintext()
                        .build();

                TaskServiceGrpc.TaskServiceBlockingStub stub =
                        TaskServiceGrpc.newBlockingStub(channel);

                AcceptRequest request = AcceptRequest.newBuilder()
                        .setProposalId(proposalId)
                        .setTaskId(taskId)
                        .setProposedNodeId(chosenNode)
                        .build();

                AcceptResponse response = stub.accept(request);

                if (response.getAccepted()) {
                    accepts++;
                    System.out.println("Accept reçu de node " + node.id);
                } else {
                    System.out.println("Accept refusé par node " + node.id);
                }

            } catch (Exception e) {
                System.out.println("Node " + node.id + " indisponible pendant ACCEPT");
            } finally {
                if (channel != null) {
                    channel.shutdown();
                }
            }
        }

        if (accepts >= majority) {
            System.out.println("Consensus atteint. Executor choisi = node " + chosenNode);
            return chosenNode;
        }

        System.out.println("Consensus échoué : pas assez d'accepts");
        return -1;
    }
}