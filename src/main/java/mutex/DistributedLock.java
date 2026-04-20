package mutex;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class DistributedLock {

    private static final ConcurrentHashMap<String, Boolean> locks = new ConcurrentHashMap<>();
    private static final Set<String> executedTasks = ConcurrentHashMap.newKeySet();

    public static boolean acquire(String taskId) {

        // 🔥 déjà exécuté
        if (executedTasks.contains(taskId)) {
            return false;
        }

        // 🔥 déjà en cours (lock)
        return locks.putIfAbsent(taskId, true) == null;
    }

    public static void release(String taskId) {
        locks.remove(taskId);

        // 🔥 marquer comme exécuté
        executedTasks.add(taskId);
    }
}