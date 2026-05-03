package consensus;

import java.util.concurrent.ConcurrentHashMap;

public class DecisionStore {

    // taskId → executorNodeId
    private static final ConcurrentHashMap<String, Integer> decisions = new ConcurrentHashMap<>();

    public static boolean hasDecision(String taskId) {
        return decisions.containsKey(taskId);
    }

    public static int getDecision(String taskId) {
        return decisions.get(taskId);
    }

    public static void saveDecision(String taskId, int nodeId) {
        decisions.put(taskId, nodeId);
    }
}