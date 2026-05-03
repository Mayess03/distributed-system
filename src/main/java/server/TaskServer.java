package server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import proto.*;
import consensus.PaxosProposer;
import consensus.DecisionStore;
import model.NodeInfo;
import mutex.DistributedLock;
import client.TaskClientGrpc;

import java.util.List;

public class TaskServer {

    private final int nodeId;
    private final int port;
    private final List<NodeInfo> nodes;

    // état Paxos local
    private int promisedId = -1;
    private int acceptedProposalId = -1;
    private int acceptedNodeId = -1;

    public TaskServer(int nodeId, int port, List<NodeInfo> nodes) {
        this.nodeId = nodeId;
        this.port = port;
        this.nodes = nodes;
    }

    public void start() throws Exception {

        Server server = ServerBuilder
                .forPort(port)
                .addService(new TaskServiceImpl())
                .build();

        server.start();

        System.out.println("Node " + nodeId + " démarré sur le port " + port);

        server.awaitTermination();
    }

    class TaskServiceImpl extends TaskServiceGrpc.TaskServiceImplBase {

        @Override
        public void submitTask(TaskRequest request,
                               StreamObserver<TaskResponse> responseObserver) {

            String taskId = request.getTaskId();
            String payload = request.getPayload();

            System.out.println("\nRéception de la tâche " + taskId + " sur node " + nodeId);

            int executorNodeId;

            // =====================================================
            // 1. CHECK : décision déjà connue
            // =====================================================
            if (DecisionStore.hasDecision(taskId)) {

                executorNodeId = DecisionStore.getDecision(taskId);

                System.out.println("Consensus déjà connu pour " + taskId +
                        " → executor = " + executorNodeId);

            } else {

                System.out.println("Consensus pas encore fait → lancement Paxos");

                PaxosProposer proposer = new PaxosProposer(nodes, nodeId);

                executorNodeId = proposer.proposeExecutor(taskId);

                if (executorNodeId == -1) {

                    responseObserver.onNext(
                            TaskResponse.newBuilder()
                                    .setStatus("FAILED")
                                    .setResult("Consensus échoué")
                                    .build()
                    );

                    responseObserver.onCompleted();
                    return;
                }

                // 🔥 SAVE DECISION (IMPORTANT)
                DecisionStore.saveDecision(taskId, executorNodeId);
            }

            // =====================================================
            // 2. REDIRECTION
            // =====================================================
            if (executorNodeId != nodeId) {

                System.out.println("Node " + nodeId +
                        " n'est pas executor → redirection vers node " +
                        executorNodeId);

                NodeInfo executor = nodes.stream()
                        .filter(n -> n.id == executorNodeId)
                        .findFirst()
                        .orElse(null);

                if (executor == null) {

                    responseObserver.onNext(
                            TaskResponse.newBuilder()
                                    .setStatus("FAILED")
                                    .setResult("Executor introuvable")
                                    .build()
                    );

                    responseObserver.onCompleted();
                    return;
                }

                TaskClientGrpc client = new TaskClientGrpc(nodes);

                TaskRequest redirected = TaskRequest.newBuilder()
                        .setTaskId(taskId)
                        .setPayload(payload)
                        .setAlreadyConsensusDone(true)
                        .build();

                TaskResponse response = client.sendToNode(redirected, executor);

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // =====================================================
            // 3. MUTEX
            // =====================================================
            if (!DistributedLock.acquire(taskId)) {

                System.out.println("Tâche déjà exécutée : " + taskId);

                responseObserver.onNext(
                        TaskResponse.newBuilder()
                                .setStatus("REFUSED")
                                .setResult("Déjà exécutée")
                                .build()
                );

                responseObserver.onCompleted();
                return;
            }

            // =====================================================
            // 4. EXECUTION
            // =====================================================
            try {

                System.out.println("Node " + nodeId +
                        " exécute la tâche " + taskId);

                Thread.sleep(1000);

                String result = payload.toUpperCase();

                System.out.println("Tâche " + taskId +
                        " exécutée par node " + nodeId);

                responseObserver.onNext(
                        TaskResponse.newBuilder()
                                .setStatus("OK")
                                .setResult("Node " + nodeId + " → " + result)
                                .build()
                );

            } catch (Exception e) {

                responseObserver.onNext(
                        TaskResponse.newBuilder()
                                .setStatus("ERROR")
                                .setResult("Erreur")
                                .build()
                );

            } finally {
                DistributedLock.release(taskId);
            }

            responseObserver.onCompleted();
        }

        // =====================================================
        // PAXOS PREPARE
        // =====================================================
        @Override
        public void prepare(PrepareRequest request,
                            StreamObserver<PrepareResponse> responseObserver) {

            boolean promise = false;

            if (request.getProposalId() > promisedId) {
                promisedId = request.getProposalId();
                promise = true;
            }

            responseObserver.onNext(
                    PrepareResponse.newBuilder()
                            .setPromise(promise)
                            .setAcceptedProposalId(acceptedProposalId)
                            .setAcceptedNodeId(acceptedNodeId)
                            .build()
            );

            responseObserver.onCompleted();
        }

        // =====================================================
        // PAXOS ACCEPT
        // =====================================================
        @Override
        public void accept(AcceptRequest request,
                           StreamObserver<AcceptResponse> responseObserver) {

            boolean accepted = false;

            if (request.getProposalId() >= promisedId) {
                promisedId = request.getProposalId();
                acceptedProposalId = request.getProposalId();
                acceptedNodeId = request.getProposedNodeId();
                accepted = true;
            }

            responseObserver.onNext(
                    AcceptResponse.newBuilder()
                            .setAccepted(accepted)
                            .build()
            );

            responseObserver.onCompleted();
        }
    }
}