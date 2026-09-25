package com.github.lisama569.distributedcoordinationservicejava.coord.sim;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.SimClock;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.ClientRequest;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.ClusterControl;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.RaftMessage;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coord.recovery.CheckpointStore;
import com.github.lisama569.distributedcoordinationservicejava.coord.statemachine.Outcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 进程内集群：共享模拟时钟与模拟网络，持有全部 {@link RaftNode}，
 * 统一按 1 模拟毫秒为步长推进时间（节点按固定顺序 tick，全程单线程），
 * 并提供分区、崩溃、从检查点重启等故障注入手段。
 */
public class Cluster implements ClusterControl {

    private static final Logger log = LoggerFactory.getLogger(Cluster.class);

    private final SimClock clock = new SimClock();
    private final Network network = new Network();
    private final Map<String, RaftNode> nodes = new java.util.LinkedHashMap<>();
    private final List<String> nodeIds;
    private final ClusterConfig config;
    private final CheckpointStore store;

    public Cluster(List<String> nodeIds, CheckpointStore store, ClusterConfig config) {
        this.nodeIds = List.copyOf(nodeIds);
        this.config = config;
        this.store = store;
        for (String id : nodeIds) {
            RaftNode node = new RaftNode(id, this.nodeIds, network, this, store, config.timers());
            node.restore();
            nodes.put(id, node);
        }
        log.info("[集群] 初始化完成: 节点={}, 多数派={}, 心跳={}ms, 选举超时=[{},{}]ms, 领导者租约={}ms",
                this.nodeIds, quorum(), config.timers().heartbeatIntervalMillis(),
                config.timers().electionTimeoutMinMillis(),
                config.timers().electionTimeoutMaxMillis(),
                config.timers().leaderLeaseMillis());
    }

    public int quorum() {
        return nodeIds.size() / 2 + 1;
    }

    public RaftNode node(String id) {
        RaftNode n = nodes.get(id);
        if (n == null) {
            throw new IllegalArgumentException("未知节点: " + id);
        }
        return n;
    }

    public List<String> nodeIds() {
        return nodeIds;
    }

    /** 当前持有有效租约且未崩溃的领导者；无则返回 null。 */
    public RaftNode leader() {
        RaftNode found = null;
        int leaderCount = 0;
        for (RaftNode n : nodes.values()) {
            if (network.isCrashed(n.getNodeId())) {
                continue;
            }
            if (n.isLeaderLeaseValid()) {
                found = n;
                leaderCount++;
            }
        }
        if (leaderCount > 1) {
            throw new IllegalStateException("检测到双主！有效租约领导者数=" + leaderCount);
        }
        return found;
    }

    public String leaderId() {
        RaftNode n = leader();
        return n == null ? null : n.getNodeId();
    }

    /** 推进指定时长（1ms 一步，依次 tick 存活节点）。 */
    public void advance(long millis) {
        for (long i = 0; i < millis; i++) {
            clock.advance(1);
            for (String id : nodeIds) {
                if (!network.isCrashed(id)) {
                    nodes.get(id).tick(clock.now());
                }
            }
        }
    }

    public boolean advanceUntil(long maxMillis, Supplier<Boolean> condition) {
        for (long i = 0; i < maxMillis; i++) {
            if (Boolean.TRUE.equals(condition.get())) {
                return true;
            }
            advance(1);
        }
        return Boolean.TRUE.equals(condition.get());
    }

    public boolean awaitLeader(long maxMillis) {
        boolean ok = advanceUntil(maxMillis, () -> leader() != null);
        if (ok) {
            log.info("[集群] 当前领导者={}, 任期={}, 租约至={}, 模拟时间={}",
                    leaderId(), leader().getCurrentTerm(), leader().getLeaseUntilMillis(), clock.now());
        }
        return ok;
    }

    public long now() {
        return clock.now();
    }

    /**
     * 通过当前领导者提交写请求。非领导者/租约过期错误由调用方决定如何处理
     * （测试可直接断言该错误，客户端门面 {@code CoordClient} 会做重试）。
     */
    public Outcome proposeThroughLeader(ClientRequest request) {
        RaftNode l = leader();
        if (l == null) {
            throw new CoordinationException(RejectReason.NO_LEADER,
                    "集群当前无有效租约领导者，无法提交: 幂等键=" + request.idempotencyKey());
        }
        return l.propose(request);
    }

    // ---------------- 故障注入 ----------------

    public void partition(String partitionName, List<String> members) {
        network.isolate(partitionName, members);
    }

    /** 把一组节点隔到独立分区，其余节点留在默认区。 */
    public void split(List<String> isolated) {
        network.healAll();
        List<String> rest = new ArrayList<>(nodeIds);
        rest.removeAll(isolated);
        network.isolate("minority", isolated);
        network.isolate("majority", rest);
    }

    public void heal() {
        network.healAll();
    }

    public void crash(String nodeId) {
        network.crash(nodeId);
    }

    /** 模拟进程重启：新建节点实例并从检查点恢复（回退会被拒绝）。 */
    public RaftNode restart(String nodeId) {
        network.recover(nodeId);
        RaftNode fresh = new RaftNode(nodeId, nodeIds, network, this,
                store, config.timers());
        fresh.restore();
        nodes.put(nodeId, fresh);
        log.info("[集群] 节点={} 已从检查点重启", nodeId);
        return fresh;
    }

    public CheckpointStore checkpointStore() {
        return store;
    }

    @Override
    public int totalNodes() {
        return nodeIds.size();
    }

    @Override
    public void send(RaftMessage message) {
        network.send(message);
    }

    @Override
    public long nowMillis() {
        return clock.now();
    }
}
