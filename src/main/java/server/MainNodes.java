package server;

public class MainNodes {
    public static void main(String[] args) {

        new TaskServer(1, 50051).start();
        new TaskServer(2, 50052).start();
        new TaskServer(3, 50053).start();
    }
}
