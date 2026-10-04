package com.github.lisama569.distributedcoordinationservicejava.cluster;

import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteCoordinator;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.core.SimClock;
import com.github.lisama569.distributedcoordinationservicejava.lock.LockService;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import com.github.lisama569.distributedcoordinationservicejava.recovery.InMemoryDisk;
import com.github.lisama569.distributedcoordinationservicejava.statemachine.KvStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 进程内集群模拟器：
 * <ul>
 *   <li>所有节点共享一个 {@link SimClock}，{@link #advance(long)} 推进一拍并让各节点 tick；</li>
 *   <li>{@link #isolate(String)} 把节点与其余节点双向隔离，模拟网络分区；{@link #healPartition()} 恢复；</li>
 *   <li>{@link #crash(String)} 丢弃节点内存对象（磁盘保留），{@link #restart(String)} 用同一磁盘重启；</li>
 *   <li>{@link #joinNewNode(String)} 以一块空白磁盘让新节点加入，由领导者通过“存档+增量”补齐；</li>
 *   <li>{@link #simulateOfflineRetentionGc} 模拟节点离线期间其本地区段被保留策略物理回收，
 *       用于制造“掉队太远、超出可恢复窗口”的场景；</li>
 *   <li>RPC 经 {@link ClusterTransport} 直接调用目标节点，分区/崩溃时返回 empty（即超时）。</li>
 * </ul>
 * 全部同步执行、无真实线程与网络，场景结论完全确定可复现。
 */
public class ClusterSimulator implements ClusterTransport {

    private static final Logger log = LoggerFactory.getLogger(ClusterSimulator.class);

    private final SimClock clock;
    private final Map<String, DurableStorage> disks = new LinkedHashMap<>();
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<String, WriteCoordinator> coordinators = new LinkedHashMap<>();
    private final Set<String> crashed = new java.util.HashSet<>();
    private final Set<String> partitioned = new java.util.HashSet<>();

    private final LockService lockService;

    private long electionTimeoutMillis = 100L;
    private long heartbeatIntervalMillis = 30L;
    private long leaseTtlMillis = 60L;
    /** 节点本地保留的已应用日志条数达到该阈值即自动压缩；<=0 关闭压缩 */
    private long compactionThresholdEntries = 8L;

    public ClusterSimulator() {
        this.clock = new SimClock(0L);
        this.lockService = new LockService(clock, leaseTtlMillis);
    }

    public ClusterSimulator(int nodeCount) {
        this();
        for (int i = 1; i <= nodeCount; i++) {
            addNode("node" + i);
        }
    }

    /** 用既有磁盘（崩溃重启）或空白磁盘（首次出现）组装节点。 */
    public void addNode(String nodeId) {
        DurableStorage disk = disks.computeIfAbsent(nodeId, k -> new InMemoryDisk());
        assembleNode(nodeId, disk);
    }

    /**
     * 全新节点加入：使用一块空白磁盘，没有任何历史。
     * 它先靠领导者的 InstallSnapshot 存档（若集群已压缩过）再补增量追上。
     */
    public void joinNewNode(String nodeId) {
        InMemoryDisk fresh = new InMemoryDisk();
        disks.put(nodeId, fresh);
        assembleNode(nodeId, fresh);
        log.info("[cluster] 新节点 {} 以空白磁盘加入，等待存档+增量追赶", nodeId);
    }

    private void assembleNode(String nodeId, DurableStorage disk) {
        // 崩溃会丢失内存状态：每次（重新）组装节点都给一个全新状态机，
        // 由 RaftNode.recover() 先装存档、再重放已提交增量重建。
        KvStateMachine sm = new KvStateMachine();
        RaftNode node = new RaftNode(nodeId, clock, disk, sm, this,
                electionTimeoutMillis, heartbeatIntervalMillis, leaseTtlMillis,
                compactionThresholdEntries);
        WriteCoordinator coordinator = new WriteCoordinator(clock, disk);
        // 恢复重放期间 listener 尚未接线：去重表只由 restore 从存档+已应用日志重建，
        // 避免把“重放”误计成新的应用。
        coordinator.restore();
        node.setAppliedListener(coordinator::onEntryApplied);
        node.setSnapshotListener(coordinator::loadFromSnapshot);
        nodes.put(nodeId, node);
        coordinators.put(nodeId, coordinator);
        crashed.remove(nodeId);
        log.info("[cluster] 节点上线 {} (term={}, snapshotIndex={}, firstIndex={})",
                nodeId, node.getCurrentTerm(), node.getSnapshotIndex(), node.getFirstLogIndex());
    }

    public LockService lockService() {
        return lockService;
    }

    // ---------------- 时间与 tick ----------------

    /** 推进逻辑时钟 deltaMillis，并让所有存活节点各 tick 一拍。 */
    public void advance(long deltaMillis) {
        clock.advance(deltaMillis);
        for (RaftNode node : new ArrayList<>(nodes.values())) {
            node.tick();
        }
    }

    /** 推进到出现一个有有效租约的领导者（带保护上限，避免死循环）。 */
    public String awaitLeader() {
        return awaitLeader(500);
    }

    public String awaitLeader(int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            RaftNode leader = leader();
            if (leader != null) {
                return leader.getNodeId();
            }
            advance(1L);
        }
        throw new CoordinationException(CoordinationErrorCode.NO_QUORUM,
                "在 " + maxTicks + " tick 内未选出有有效租约的领导者，存活节点="
                        + nodes.keySet() + " 分区节点=" + partitioned);
    }

    /** 返回当前唯一有有效租约的领导者；没有或身份不确定时返回 null。 */
    public RaftNode leader() {
        RaftNode found = null;
        long now = clock.nowMillis();
        for (RaftNode node : nodes.values()) {
            if (node.isLeader() && node.hasValidLease(now)) {
                if (found != null) {
                    // 双主在设计上不应发生；一旦出现直接暴露而不是静默选一个
                    throw new CoordinationException(CoordinationErrorCode.INTERNAL,
                            "检测到两个自认为有效的领导者: " + found.getNodeId()
                                    + "(term=" + found.getCurrentTerm() + ") 与 "
                                    + node.getNodeId() + "(term=" + node.getCurrentTerm() + ")");
                }
                found = node;
            }
        }
        return found;
    }

    // ---------------- 崩溃与恢复 ----------------

    /** 模拟崩溃：丢弃节点内存对象与协调器，磁盘与状态机保留。 */
    public void crash(String nodeId) {
        RaftNode removed = nodes.remove(nodeId);
        coordinators.remove(nodeId);
        crashed.add(nodeId);
        if (removed != null) {
            log.info("[cluster] 节点崩溃 {} (磁盘保留: first={}, last={}, snapshotK={})",
                    nodeId, disks.get(nodeId).firstIndex(), disks.get(nodeId).lastIndex(),
                    removed.getSnapshotIndex());
        }
    }

    /** 用同一块磁盘重启节点：先装存档、再补增量恢复，并重建幂等去重表。 */
    public void restart(String nodeId) {
        if (!disks.containsKey(nodeId)) {
            throw new IllegalArgumentException("未知节点，无法重启: " + nodeId);
        }
        addNode(nodeId);
        log.info("[cluster] 节点重启完成 {} term={} first={} last={}",
                nodeId, node(nodeId).getCurrentTerm(),
                disks.get(nodeId).firstIndex(), disks.get(nodeId).lastIndex());
    }

    /**
     * 测试钩子：模拟节点离线（崩溃）期间，其本地 [1,upToIndex] 区段被保留策略
     * 物理回收。磁盘仍能加载存档，但存档之后被回收的位点无法再提供，
     * 用来制造“掉队太远、超出可恢复窗口”的节点。
     */
    public void simulateOfflineRetentionGc(String nodeId, long upToIndex) {
        DurableStorage disk = disks.get(nodeId);
        if (!(disk instanceof InMemoryDisk mem)) {
            throw new IllegalStateException("仅 InMemoryDisk 支持模拟回收");
        }
        mem.simulateOfflineRetentionGcUpTo(upToIndex);
        log.info("[cluster] 模拟节点 {} 离线期间本地日志被物理回收至 index={}（现存 first={}, last={}, snapshotK={}）",
                nodeId, upToIndex, disk.firstIndex(), disk.lastIndex(),
                disk.loadSnapshot().map(s -> s.lastIncludedIndex()).orElse(0L));
    }

    // ---------------- 网络分区 ----------------

    /** 将 nodeId 与其他所有节点双向隔离。 */
    public void isolate(String nodeId) {
        partitioned.add(nodeId);
        log.info("[cluster] 施加网络分区: {} 与集群隔离 @t={}", nodeId, clock.nowMillis());
    }

    public void healPartition() {
        if (!partitioned.isEmpty()) {
            log.info("[cluster] 分区恢复: {} 重新连通 @t={}", partitioned, clock.nowMillis());
        }
        partitioned.clear();
    }

    public boolean isPartitioned(String nodeId) {
        return partitioned.contains(nodeId);
    }

    // ---------------- ClusterTransport ----------------

    @Override
    public Collection<String> nodeIds() {
        return nodes.keySet();
    }

    @Override
    public Optional<VoteResponse> sendRequestVote(String from, String to, VoteRequest request) {
        RaftNode target = nodes.get(to);
        if (target == null || unreachable(from, to)) {
            return Optional.empty();
        }
        return Optional.of(target.handleRequestVote(request));
    }

    @Override
    public Optional<AppendResponse> sendAppendEntries(String from, String to, AppendRequest request) {
        RaftNode target = nodes.get(to);
        if (target == null || unreachable(from, to)) {
            return Optional.empty();
        }
        return Optional.of(target.handleAppendEntries(request));
    }

    @Override
    public Optional<InstallSnapshotResponse> sendInstallSnapshot(String from, String to,
                                                                 InstallSnapshotRequest request) {
        RaftNode target = nodes.get(to);
        if (target == null || unreachable(from, to)) {
            return Optional.empty();
        }
        return Optional.of(target.handleInstallSnapshot(request));
    }

    private boolean unreachable(String a, String b) {
        return partitioned.contains(a) || partitioned.contains(b)
                || crashed.contains(a) || crashed.contains(b);
    }

    // ---------------- 访问 ----------------

    public SimClock clock() {
        return clock;
    }

    public RaftNode node(String nodeId) {
        RaftNode n = nodes.get(nodeId);
        if (n == null) {
            throw new CoordinationException(CoordinationErrorCode.NOT_LEADER,
                    "节点 " + nodeId + " 当前已崩溃/不存在（内存状态已丢失）");
        }
        return n;
    }

    public List<RaftNode> nodes() {
        return new ArrayList<>(nodes.values());
    }

    public WriteCoordinator coordinator(String nodeId) {
        return coordinators.get(nodeId);
    }

    public DurableStorage disk(String nodeId) {
        return disks.get(nodeId);
    }

    public long getElectionTimeoutMillis() {
        return electionTimeoutMillis;
    }

    public void setElectionTimeoutMillis(long v) {
        this.electionTimeoutMillis = v;
    }

    public long getHeartbeatIntervalMillis() {
        return heartbeatIntervalMillis;
    }

    public void setHeartbeatIntervalMillis(long v) {
        this.heartbeatIntervalMillis = v;
    }

    public long getLeaseTtlMillis() {
        return leaseTtlMillis;
    }

    public void setLeaseTtlMillis(long v) {
        this.leaseTtlMillis = v;
    }

    public long getCompactionThresholdEntries() {
        return compactionThresholdEntries;
    }

    public void setCompactionThresholdEntries(long v) {
        this.compactionThresholdEntries = v;
    }
}
