package server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import proto.*;
import consensus.PaxosProposer;
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

            System.out.println("\nRéception de la tâche "
                    + taskId + " sur le node " + nodeId);

            int executorNodeId;

            // =================================================
            // 1. Lancer Paxos uniquement si pas encore fait
            // =================================================
            if (!request.getAlreadyConsensusDone()) {

                System.out.println("Consensus pas encore fait → lancement Paxos");

                PaxosProposer proposer =
                        new PaxosProposer(nodes, nodeId);

                executorNodeId =
                        proposer.proposeExecutor(taskId);

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

            } else {

                System.out.println(
                        "Consensus déjà fait → exécution directe");

                executorNodeId = nodeId;
            }

            // =================================================
            // 2. Redirection vers executor
            // =================================================
            if (executorNodeId != nodeId) {

                System.out.println(
                        "Node " + nodeId +
                                " n'est pas l'executor choisi");

                System.out.println(
                        "Redirection vers node "
                                + executorNodeId);

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

                TaskClientGrpc client =
                        new TaskClientGrpc(nodes);

                // IMPORTANT → éviter relance Paxos
                TaskRequest redirectedRequest =
                        TaskRequest.newBuilder()
                                .setTaskId(request.getTaskId())
                                .setPayload(request.getPayload())
                                .setAlreadyConsensusDone(true)
                                .build();

                TaskResponse response =
                        client.sendToNode(
                                redirectedRequest,
                                executor
                        );

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // =================================================
            // 3. Mutex
            // =================================================
            if (!DistributedLock.acquire(taskId)) {

                System.out.println(
                        "Tâche " + taskId +
                                " déjà exécutée ou en cours");

                responseObserver.onNext(
                        TaskResponse.newBuilder()
                                .setStatus("REFUSE")
                                .setResult("Tâche déjà exécutée")
                                .build()
                );

                responseObserver.onCompleted();
                return;
            }

            // =================================================
            // 4. Exécution
            // =================================================
            try {

                System.out.println(
                        "Node " + nodeId +
                                " exécute la tâche " + taskId);

                Thread.sleep(1000);

                String result = payload.toUpperCase();

                System.out.println(
                        "Tâche " + taskId +
                                " exécutée avec succès par node "
                                + nodeId);

                responseObserver.onNext(
                        TaskResponse.newBuilder()
                                .setStatus("OK")
                                .setResult(
                                        "Résultat du node "
                                                + nodeId
                                                + " : "
                                                + result
                                )
                                .build()
                );

            } catch (Exception e) {

                responseObserver.onNext(
                        TaskResponse.newBuilder()
                                .setStatus("ERREUR")
                                .setResult("Erreur d'exécution")
                                .build()
                );

            } finally {
                DistributedLock.release(taskId);
            }

            responseObserver.onCompleted();
        }

        // =========================================
        // PHASE 1 : PREPARE
        // =========================================
        @Override
        public void prepare(PrepareRequest request,
                            StreamObserver<PrepareResponse> responseObserver) {

            int proposalId = request.getProposalId();

            System.out.println(
                    "Node " + nodeId +
                            " reçoit PREPARE : "
                            + proposalId);

            boolean promise = false;

            if (proposalId > promisedId) {
                promisedId = proposalId;
                promise = true;
            }

            PrepareResponse response =
                    PrepareResponse.newBuilder()
                            .setPromise(promise)
                            .setAcceptedProposalId(
                                    acceptedProposalId
                            )
                            .setAcceptedNodeId(
                                    acceptedNodeId
                            )
                            .build();

            if (promise) {
                System.out.println(
                        "Node " + nodeId +
                                " → PROMISE"
                );
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // =========================================
        // PHASE 2 : ACCEPT
        // =========================================
        @Override
        public void accept(AcceptRequest request,
                           StreamObserver<AcceptResponse> responseObserver) {

            int proposalId = request.getProposalId();
            int proposedNodeId =
                    request.getProposedNodeId();

            System.out.println(
                    "Node " + nodeId +
                            " reçoit ACCEPT pour node "
                            + proposedNodeId
            );

            boolean accepted = false;

            if (proposalId >= promisedId) {
                acceptedProposalId = proposalId;
                acceptedNodeId = proposedNodeId;
                accepted = true;
            }

            AcceptResponse response =
                    AcceptResponse.newBuilder()
                            .setAccepted(accepted)
                            .build();

            if (accepted) {
                System.out.println(
                        "Node " + nodeId +
                                " accepte executor "
                                + proposedNodeId
                );
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}