package server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import proto.TaskServiceGrpc;
import proto.TaskRequest;
import proto.TaskResponse;

import consensus.LeaderElection;
import model.NodeInfo;
import mutex.DistributedLock;
import client.TaskClientGrpc;

import java.util.List;

public class TaskServer {

    private final int nodeId;
    private final int port;
    private final List<NodeInfo> nodes;

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
        public void submitTask(TaskRequest request, StreamObserver<TaskResponse> responseObserver) {

            String taskId = request.getTaskId();
            String payload = request.getPayload();

            NodeInfo leader = LeaderElection.electLeader(nodes);

            System.out.println("\nRéception de la tâche " + taskId + " sur le node " + nodeId);

            // Cas 1 : redirection vers le leader
            if (leader.id != nodeId) {

                System.out.println("Node " + nodeId + " n'est pas le leader.");
                System.out.println("Redirection vers le leader node " + leader.id);

                TaskClientGrpc client = new TaskClientGrpc(nodes);
                TaskResponse response = client.sendToLeader(request);

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }

            // Cas 2 : verrou déjà pris ou tâche déjà exécutée
            if (!DistributedLock.acquire(taskId)) {

                System.out.println("Tâche " + taskId + " déjà exécutée ou en cours d'exécution sur le node " + nodeId);

                responseObserver.onNext(
                        TaskResponse.newBuilder()
                                .setStatus("REFUSE")
                                .setResult("Tâche déjà exécutée")
                                .build()
                );

                responseObserver.onCompleted();
                return;
            }

            try {
                System.out.println("Node " + nodeId + " exécute la tâche " + taskId);

                Thread.sleep(1000); // simulation de calcul

                String result = payload.toUpperCase();

                System.out.println("Tâche " + taskId + " exécutée avec succès par le node " + nodeId);

                responseObserver.onNext(
                        TaskResponse.newBuilder()
                                .setStatus("OK")
                                .setResult("Résultat du node " + nodeId + " : " + result)
                                .build()
                );

            } catch (Exception e) {

                System.out.println("Erreur lors de l'exécution de la tâche " + taskId);

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
    }
}