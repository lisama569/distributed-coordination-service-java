package com.github.lisama569.distributedcoordinationservicejava.cluster;

import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.core.SimClock;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Checkpoint;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import com.github.lisama569.distributedcoordinationservicejava.statemachine.KvStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 单个 Raft 风格节点，使用共享的逻辑时钟驱动，全部在进程内运行，不涉及真实网络与线程。
 *
 * <h3>选举与任期</h3>
 * <ul>
 *   <li>选举超时未收到合法领导者心跳 -> term+1、投自己、向多数派拉票；</li>
 *   <li>同一 term 只能投一票，日志不够新不给票，因此同一任期至多产生一个领导者；</li>
 *   <li>收到更高 term 的任何 RPC 都立即退让为 follower，任期只升不降。</li>
 * </ul>
 *
 * <h3>领导者租约</h3>
 * <ul>
 *   <li>每 {@code heartbeatInterval} 发一轮心跳；得到多数派 ACK 即把租约续到 now+TTL；</li>
 *   <li>失去多数派后 TTL 内无法续约，{@link #propose} 一律以 LEASE_EXPIRED/NOT_LEADER 拒绝；</li>
 *   <li>分区恢复时旧领导者一旦收到更高 term 响应/请求立即下台，绝不以旧 term 接受写。</li>
 * </ul>
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final SimClock clock;
    private final DurableStorage storage;
    private final KvStateMachine stateMachine;
    private final ClusterTransport transport;
    private final long electionTimeoutMillis;
    private final long heartbeatIntervalMillis;
    private final long leaseTtlMillis;
    /** 每个节点固定的选举超时扰动，避免同时超时导致分票 */
    private final long electionJitter;

    // ---- 持久化状态（内存镜像，权威数据在 storage） ----
    private long currentTerm;
    private String votedFor;

    // ---- 易失状态 ----
    private NodeRole role = NodeRole.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadline;

    // ---- 领导者状态 ----
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Set<String> votesGranted = new HashSet<>();
    private long leaseExpireAtMillis = Long.MIN_VALUE;
    private long nextHeartbeatAt;

    public RaftNode(String nodeId, SimClock clock, DurableStorage storage,
                    KvStateMachine stateMachine, ClusterTransport transport,
                    long electionTimeoutMillis, long heartbeatIntervalMillis,
                    long leaseTtlMillis) {
        this.nodeId = nodeId;
        this.clock = clock;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.electionTimeoutMillis = electionTimeoutMillis;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.leaseTtlMillis = leaseTtlMillis;
        // 以节点号做确定性扰动，保证各节点超时时刻错开且可重现
        this.electionJitter = Math.floorMod(nodeId.hashCode(), 25L);
        recover();
        resetElectionTimer();
    }

    // ==================== 崩溃恢复 ====================

    private void recover() {
        Checkpoint cp = storage.loadCheckpoint();
        this.currentTerm = cp.currentTerm();
        this.votedFor = cp.votedFor();
        this.commitIndex = cp.appliedIndex();
        this.lastApplied = cp.appliedIndex();
        // 把已提交日志按顺序重放到状态机；同一位点重放不产生重复效果
        for (long i = 1L; i <= lastApplied; i++) {
            LogEntry e = storage.loadEntry(i);
            if (e != null) {
                stateMachine.apply(i, keyOf(e), e.value());
            }
        }
        this.role = NodeRole.FOLLOWER;
        this.leaderId = null;
        this.leaseExpireAtMillis = Long.MIN_VALUE;
        log.info("[recover node={}] 从检查点恢复: term={}, lastIndex={}, appliedIndex={}",
                nodeId, currentTerm, storage.lastIndex(), lastApplied);
    }

    // ==================== 时钟驱动 ====================

    private void resetElectionTimer() {
        this.electionDeadline = clock.nowMillis() + electionTimeoutMillis + electionJitter;
    }

    /** 逻辑时钟每走一拍由集群模拟器调用：处理选举超时与心跳。 */
    public void tick() {
        long now = clock.nowMillis();
        if (role == NodeRole.LEADER) {
            if (now >= nextHeartbeatAt) {
                broadcastHeartbeats();
                nextHeartbeatAt = now + heartbeatIntervalMillis;
            }
            // 租约到期且无法续约（上面的心跳若拿到多数派会续约）-> 主动下台
            if (now > leaseExpireAtMillis) {
                log.info("[lease node={}] 领导者租约过期 term={} now={} expireAt={}，下台",
                        nodeId, currentTerm, now, leaseExpireAtMillis);
                becomeFollower(currentTerm, null);
            }
            return;
        }
        if (now >= electionDeadline) {
            startElection();
        }
    }

    // ==================== 选举 ====================

    private void startElection() {
        currentTerm++;
        role = NodeRole.CANDIDATE;
        leaderId = null;
        votedFor = nodeId;
        votesGranted.clear();
        votesGranted.add(nodeId);
        persistTermAndVote();
        resetElectionTimer();
        log.info("[elect node={}] 选举超时，开始拉票 term={} lastLogIndex={} lastLogTerm={}",
                nodeId, currentTerm, localLastIndex(), localLastTerm());

        VoteRequest request = new VoteRequest(currentTerm, nodeId, localLastIndex(), localLastTerm());
        int total = transport.nodeIds().size();
        int granted = 1;
        for (String peer : transport.nodeIds()) {
            if (peer.equals(nodeId)) {
                continue;
            }
            Optional<VoteResponse> reply = transport.sendRequestVote(nodeId, peer, request);
            if (reply.isEmpty()) {
                // 被分区隔离 / 丢包：视为未投票
                log.debug("[elect node={}] -> {} 不可达（分区）", nodeId, peer);
                continue;
            }
            VoteResponse resp = reply.get();
            if (resp.term() > currentTerm) {
                becomeFollower(resp.term(), null);
                return;
            }
            if (role != NodeRole.CANDIDATE) {
                return;
            }
            if (resp.granted()) {
                granted++;
            }
        }
        if (role != NodeRole.CANDIDATE) {
            return;
        }
        log.info("[elect node={}] term={} 得票 {}/{}", nodeId, currentTerm, granted, total);
        if (granted * 2 > total) {
            becomeLeader();
        }
        // 否则保持 candidate，等待下一轮超时（term 再 +1）
    }

    private void becomeLeader() {
        role = NodeRole.LEADER;
        leaderId = nodeId;
        nextIndex.clear();
        matchIndex.clear();
        for (String peer : transport.nodeIds()) {
            if (!peer.equals(nodeId)) {
                nextIndex.put(peer, localLastIndex() + 1L);
                matchIndex.put(peer, 0L);
            }
        }
        // 当选即发一轮心跳：既宣告领导权，也尝试立刻获得多数派 ACK 以建立租约
        broadcastHeartbeats();
        nextHeartbeatAt = clock.nowMillis() + heartbeatIntervalMillis;
        log.info("[elect node={}] 成为领导者 term={} 集群规模={}",
                nodeId, currentTerm, transport.nodeIds().size());
    }

    private void becomeFollower(long term, String newLeaderId) {
        if (term < currentTerm) {
            throw new CoordinationException(CoordinationErrorCode.STALE_TERM,
                    "拒绝回退到旧任期: currentTerm=" + currentTerm + ", requestedTerm=" + term);
        }
        boolean termAdvanced = term > currentTerm;
        if (termAdvanced) {
            currentTerm = term;
            votedFor = null;
            persistTermAndVote();
        }
        // 任期推进（旧领导信息必然过时）或本节点被同 term 要求下台时，清空已知领导者
        if (newLeaderId != null) {
            leaderId = newLeaderId;
        } else if (termAdvanced || role == NodeRole.LEADER) {
            leaderId = null;
        }
        role = NodeRole.FOLLOWER;
        leaseExpireAtMillis = Long.MIN_VALUE;
        votesGranted.clear();
        resetElectionTimer();
        log.info("[elect node={}] 转为 follower term={} leader={}",
                nodeId, currentTerm, newLeaderId);
    }

    public VoteResponse handleRequestVote(VoteRequest request) {
        if (request.term() < currentTerm) {
            log.info("[vote node={}] 拒绝给 {} 投票: 请求任期 {} < 当前 {}",
                    nodeId, request.candidateId(), request.term(), currentTerm);
            return new VoteResponse(currentTerm, false);
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), null);
        }
        boolean logUpToDate = request.lastLogTerm() > localLastTerm()
                || (request.lastLogTerm() == localLastTerm()
                    && request.lastLogIndex() >= localLastIndex());
        boolean canVote = (votedFor == null || votedFor.equals(request.candidateId()))
                && logUpToDate;
        if (canVote) {
            votedFor = request.candidateId();
            persistTermAndVote();
            resetElectionTimer();
            log.info("[vote node={}] 同意给 {} 投票 term={} (lastLogIndex={}, lastLogTerm={})",
                    nodeId, request.candidateId(), currentTerm,
                    request.lastLogIndex(), request.lastLogTerm());
        } else {
            log.info("[vote node={}] 拒绝给 {} 投票 term={} votedFor={} logUpToDate={}",
                    nodeId, request.candidateId(), currentTerm, votedFor, logUpToDate);
        }
        return new VoteResponse(currentTerm, canVote);
    }

    public void handleVoteResponse(String fromId, VoteResponse response) {
        if (response.term() > currentTerm) {
            becomeFollower(response.term(), null);
        }
    }

    // ==================== 心跳与复制 ====================

    private void broadcastHeartbeats() {
        int total = transport.nodeIds().size();
        int acks = 1; // 自己
        for (String peer : transport.nodeIds()) {
            if (peer.equals(nodeId)) {
                continue;
            }
            long ni = nextIndex.getOrDefault(peer, localLastIndex() + 1L);
            long prevLogIndex = ni - 1L;
            long prevLogTerm = prevLogIndex == 0L ? 0L : termAt(prevLogIndex);
            List<LogEntry> batch = storage.loadEntries(ni);
            AppendRequest request = new AppendRequest(currentTerm, nodeId, prevLogIndex,
                    prevLogTerm, commitIndex, batch);
            Optional<AppendResponse> reply = transport.sendAppendEntries(nodeId, peer, request);
            if (reply.isEmpty()) {
                continue;
            }
            AppendResponse resp = reply.get();
            if (resp.term() > currentTerm) {
                becomeFollower(resp.term(), null);
                return;
            }
            if (role != NodeRole.LEADER) {
                return;
            }
            if (resp.success()) {
                acks++;
                long matched = prevLogIndex + batch.size();
                matchIndex.put(peer, matched);
                nextIndex.put(peer, matched + 1L);
            } else {
                // 对端日志落后或冲突：nextIndex 回退一格，下一轮继续追
                nextIndex.put(peer, Math.max(1L, ni - 1L));
            }
        }
        if (role != NodeRole.LEADER) {
            return;
        }
        if (acks * 2 > total) {
            leaseExpireAtMillis = clock.nowMillis() + leaseTtlMillis;
            advanceCommitIndex();
        }
        applyCommitted();
    }

    public AppendResponse handleAppendEntries(AppendRequest request) {
        if (request.term() < currentTerm) {
            return new AppendResponse(currentTerm, false, 0L);
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), request.leaderId());
        } else {
            // 同 term：承认对端领导权（同一 term 至多一个领导者）
            role = NodeRole.FOLLOWER;
            leaderId = request.leaderId();
            resetElectionTimer();
        }
        long prevIndex = request.prevLogIndex();
        long prevTerm = request.prevLogTerm();
        if (prevIndex > 0L) {
            LogEntry prev = storage.loadEntry(prevIndex);
            if (prev == null || prev.term() != prevTerm) {
                log.debug("[append node={}] 拒绝: prevLog[{}/term={}] 不匹配",
                        nodeId, prevIndex, prevTerm);
                return new AppendResponse(currentTerm, false, 0L);
            }
        }
        long baseIndex = prevIndex;
        for (LogEntry incoming : request.entries()) {
            long idx = baseIndex + 1L;
            LogEntry existing = storage.loadEntry(idx);
            if (existing == null) {
                storage.appendEntry(new LogEntry(idx, incoming.term(),
                        incoming.idempotencyKey(), incoming.value()));
            } else if (existing.term() != incoming.term()) {
                // 冲突：本模拟没有截断 API，而单领导者顺序提交下不会走到这里；
                // 显式拒绝比静默覆盖安全。
                throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                        "日志在 index=" + idx + " 处任期冲突: existing=" + existing.term()
                                + ", incoming=" + incoming.term());
            }
            // existing 且 term 相同：幂等忽略（重复投递）
            baseIndex = idx;
        }
        if (request.leaderCommit() > commitIndex) {
            commitIndex = Math.min(request.leaderCommit(), storage.lastIndex());
            persistAppliedCheckpoint();
        }
        applyCommitted();
        long matched = prevIndex + request.entries().size();
        return new AppendResponse(currentTerm, true, matched);
    }

    public void handleAppendResponse(String fromId, AppendResponse response) {
        if (response.term() > currentTerm) {
            becomeFollower(response.term(), null);
        }
    }

    private void advanceCommitIndex() {
        long candidate = commitIndex;
        for (long n = localLastIndex(); n > commitIndex; n--) {
            int count = 1; // 领导者自己拥有
            for (long m : matchIndex.values()) {
                if (m >= n) {
                    count++;
                }
            }
            // Raft 安全约束：只提交当前 term 的条目（间接保证旧 term 条目随之提交）
            if (count * 2 > transport.nodeIds().size() && termAt(n) == currentTerm) {
                candidate = n;
                break;
            }
        }
        if (candidate > commitIndex) {
            commitIndex = candidate;
            persistAppliedCheckpoint();
            log.info("[commit node={}] 提交位点推进 commitIndex={} term={}",
                    nodeId, commitIndex, currentTerm);
        }
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            long idx = lastApplied + 1L;
            LogEntry entry = storage.loadEntry(idx);
            if (entry == null) {
                break;
            }
            String appliedValue = stateMachine.apply(idx, keyOf(entry), entry.value());
            lastApplied = idx;
            persistAppliedCheckpoint();
            log.info("[apply node={}] 应用日志 index={} term={} idempotencyKey={} value={}",
                    nodeId, idx, entry.term(), entry.idempotencyKey(), appliedValue);
        }
    }

    // ==================== 写入 ====================

    /**
     * 领导者接收写请求：校验身份与租约 -> 本地持久化 -> 复制到多数派 -> 提交应用。
     */
    public LogEntry propose(String idempotencyKey, String value) {
        long now = clock.nowMillis();
        if (role != NodeRole.LEADER) {
            throw new CoordinationException(CoordinationErrorCode.NOT_LEADER,
                    "拒绝写入: 节点 " + nodeId + " 不是领导者 (role=" + role
                            + ", term=" + currentTerm + ")");
        }
        if (now > leaseExpireAtMillis) {
            throw new CoordinationException(CoordinationErrorCode.LEASE_EXPIRED,
                    "拒绝写入: 领导者租约已过期 node=" + nodeId + " term=" + currentTerm
                            + " now=" + now + " expireAt=" + leaseExpireAtMillis);
        }
        long index = storage.lastIndex() + 1L;
        LogEntry entry = new LogEntry(index, currentTerm, idempotencyKey, value);
        storage.appendEntry(entry);

        // 立即同步复制一轮：多数派 ACK 则续约+提交；否则按失去多数派处理
        broadcastHeartbeats();

        if (role != NodeRole.LEADER) {
            throw new CoordinationException(CoordinationErrorCode.NOT_LEADER,
                    "写入过程中发现更高任期，节点已下台 node=" + nodeId);
        }
        if (commitIndex < index) {
            throw new CoordinationException(CoordinationErrorCode.NO_QUORUM,
                    "写入未获多数派确认 node=" + nodeId + " term=" + currentTerm
                            + " index=" + index + " commitIndex=" + commitIndex);
        }
        log.info("[write node={}] 写入已提交 index={} term={} idempotencyKey={}",
                nodeId, index, currentTerm, idempotencyKey);
        return storage.loadEntry(index);
    }

    // ==================== 辅助 ====================

    private String keyOf(LogEntry entry) {
        return entry.idempotencyKey() != null ? entry.idempotencyKey() : ("idx-" + entry.index());
    }

    private long localLastIndex() {
        return storage.lastIndex();
    }

    private long localLastTerm() {
        return storage.lastTerm();
    }

    private long termAt(long index) {
        LogEntry e = storage.loadEntry(index);
        return e == null ? 0L : e.term();
    }

    private void persistTermAndVote() {
        // 仅推进任期/投票，日志与应用位点保持磁盘权威值，杜绝把未应用数据冒充已应用
        Checkpoint cur = storage.loadCheckpoint();
        storage.saveCheckpoint(new Checkpoint(currentTerm, votedFor,
                Math.max(cur.lastIndex(), storage.lastIndex()), cur.appliedIndex()));
    }

    private void persistAppliedCheckpoint() {
        // appliedIndex 只随状态机真实应用推进；commitIndex 可能领先 appliedIndex
        storage.saveCheckpoint(new Checkpoint(currentTerm, votedFor,
                storage.lastIndex(), lastApplied));
    }

    // ==================== 只读访问 ====================

    public String getNodeId() {
        return nodeId;
    }

    public NodeRole getRole() {
        return role;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public long getLastApplied() {
        return lastApplied;
    }

    public boolean isLeader() {
        return role == NodeRole.LEADER;
    }

    /** 领导者租约是否仍有效（租约内允许服务读与判定）。 */
    public boolean hasValidLease(long nowMillis) {
        return role == NodeRole.LEADER && nowMillis <= leaseExpireAtMillis;
    }

    public long getLeaseExpireAtMillis() {
        return leaseExpireAtMillis;
    }

    public List<LogEntry> logView() {
        return storage.loadEntries(1L);
    }

    public static UnsupportedOperationException todo() {
        return new UnsupportedOperationException("尚未实现");
    }
}
