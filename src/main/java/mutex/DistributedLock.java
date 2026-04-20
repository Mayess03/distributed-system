package mutex;

import java.util.concurrent.ConcurrentHashMap;

public class DistributedLock {

    private static final ConcurrentHashMap<String, Boolean> locks = new ConcurrentHashMap<>();

    public static boolean acquire(String taskId) {
        return locks.putIfAbsent(taskId, true) == null;
    }

    public static void release(String taskId) {
        locks.remove(taskId);
    }
}