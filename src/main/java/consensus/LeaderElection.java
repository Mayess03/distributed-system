package consensus;
import model.NodeInfo;

import java.util.List;

public class LeaderElection {

    public static NodeInfo electLeader(List<NodeInfo> nodes) {
        return nodes.stream()
                .min((a, b) -> Integer.compare(a.id, b.id))
                .orElse(null);
    }
}
