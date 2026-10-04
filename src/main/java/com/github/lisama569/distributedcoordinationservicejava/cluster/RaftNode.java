package com.github.lisama569.distributedcoordinationservicejava.cluster;

import com.github.lisama569.distributedcoordinationservicejava.compaction.LogCompactor;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.core.SimClock;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Checkpoint;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Snapshot;
import com.github.lisama569.distributedcoordinationservicejava.statemachine.KvStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

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
 *
 * <h3>历史压缩与追赶</h3>
 * <ul>
 *   <li>已应用日志达到可配置阈值后，{@link LogCompactor} 把较早历史整理成存档，
 *       存档只包含已提交并应用的内容；</li>
 *   <li>恢复顺序固定为“先安装存档 -> 再补存档位点之后的增量”，结果与完整重放一致；</li>
 *   <li>心跳发现对端所需位点已被自己压缩时改用 InstallSnapshot 发完整存档；
 *       追赶期间不阻塞正常写入，每条增量只应用一次；</li>
 *   <li>超出可恢复窗口（拿到的存档比本地旧、或本地历史出现无法跨越的空洞）
 *       一律以 {@code BEYOND_RECOVERY_WINDOW} 显式拒绝，不假装追平、不从中间位点续传。</li>
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
    private final long compactionThresholdEntries;
    /** 每个节点固定的选举超时扰动，避免同时超时导致分票 */
    private final long electionJitter;

    /** 实时应用/安装存档的监听回调（由集群模拟器接到 WriteCoordinator，恢复重放不回调） */
    private Consumer<LogEntry> appliedListener;
    private Consumer<Snapshot> snapshotListener;

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
        this(nodeId, clock, storage, stateMachine, transport,
                electionTimeoutMillis, heartbeatIntervalMillis, leaseTtlMillis, 0L);
    }

    public RaftNode(String nodeId, SimClock clock, DurableStorage storage,
                    KvStateMachine stateMachine, ClusterTransport transport,
                    long electionTimeoutMillis, long heartbeatIntervalMillis,
                    long leaseTtlMillis, long compactionThresholdEntries) {
        this.nodeId = nodeId;
        this.clock = clock;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.electionTimeoutMillis = electionTimeoutMillis;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.leaseTtlMillis = leaseTtlMillis;
        this.compactionThresholdEntries = compactionThresholdEntries;
        // 以节点号做确定性扰动，保证各节点超时时刻错开且可重现
        this.electionJitter = Math.floorMod(nodeId.hashCode(), 25L);
        recover();
        resetElectionTimer();
    }

    /** 注册实时应用/存档回调（在节点参与集群前由模拟器接线）。 */
    public void setAppliedListener(Consumer<LogEntry> appliedListener) {
        this.appliedListener = appliedListener;
    }

    public void setSnapshotListener(Consumer<Snapshot> snapshotListener) {
        this.snapshotListener = snapshotListener;
    }

    // ==================== 崩溃恢复 ====================

    private void recover() {
        Checkpoint cp = storage.loadCheckpoint();
        this.currentTerm = cp.currentTerm();
        this.votedFor = cp.votedFor();

        // 固定恢复顺序：先安装存档（若有），再按序重放之后的已提交增量。
        Optional<Snapshot> snap = storage.loadSnapshot();
        long replayFrom = 1L;
        if (snap.isPresent()) {
            Snapshot s = snap.get();
            stateMachine.installSnapshot(s);
            replayFrom = s.lastIncludedIndex() + 1L;
            log.info("[recover node={}] 先加载历史存档: snapshotIndex={} term={} 状态键={} 幂等记录={}",
                    nodeId, s.lastIncludedIndex(), s.lastIncludedTerm(),
                    s.state().size(), s.idempotent().size());
        }
        this.commitIndex = cp.appliedIndex();
        this.lastApplied = cp.appliedIndex();

        // 无存档时只能从 index=1 开始重放；有存档时从存档边界之后重放。
        // 任何一段缺失都意味着本地历史存在无法跨越的空洞，必须显式拒绝恢复，
        // 而不是静默跳过（否则等于假装追平 / 从中间位点续传）。
        for (long i = replayFrom; i <= lastApplied; i++) {
            LogEntry e = storage.loadEntry(i);
            if (e == null) {
                throw new CoordinationException(CoordinationErrorCode.BEYOND_RECOVERY_WINDOW,
                        "恢复失败: 所需 index=" + i + " 的历史已超出保留窗口（存档位点="
                                + (replayFrom - 1L) + ", 现存 first=" + storage.firstIndex()
                                + ", last=" + storage.lastIndex()
                                + "），禁止假装追平或从中间位点续传");
            }
            stateMachine.apply(i, keyOf(e), e.value());
        }
        this.role = NodeRole.FOLLOWER;
        this.leaderId = null;
        this.leaseExpireAtMillis = Long.MIN_VALUE;
        log.info("[recover node={}] 恢复完成: term={}, 保留窗口[first={}, last={}], appliedIndex={}",
                nodeId, currentTerm, storage.firstIndex(), storage.lastIndex(), lastApplied);
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
                matchIndex.put(peer, snapshotIndex());
            }
        }
        // 当选即发一轮心跳：既宣告领导权，也尝试立刻获得多数派 ACK 以建立租约
        broadcastHeartbeats();
        nextHeartbeatAt = clock.nowMillis() + heartbeatIntervalMillis;
        log.info("[elect node={}] 成为领导者 term={} 集群规模={} 存档位点={}",
                nodeId, currentTerm, transport.nodeIds().size(), snapshotIndex());
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

    // ==================== 心跳、复制与存档追赶 ====================

    private void broadcastHeartbeats() {
        int total = transport.nodeIds().size();
        int acks = 1; // 自己
        for (String peer : transport.nodeIds()) {
            if (peer.equals(nodeId)) {
                continue;
            }
            long ni = nextIndex.getOrDefault(peer, localLastIndex() + 1L);

            // 对端下一条需要的位点已经被本节点压缩掉：只能用完整存档追赶
            if (ni <= snapshotIndex()) {
                if (!sendSnapshotToPeer(peer)) {
                    continue;
                }
                acks++;
                continue;
            }

            long prevLogIndex = ni - 1L;
            long prevLogTerm = termAt(prevLogIndex);
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
                long peerLast = resp.lastLogIndex();
                if (peerLast < snapshotIndex()) {
                    // 对端的历史已落在本节点存档覆盖范围内 -> 下一轮直接发存档
                    nextIndex.put(peer, 1L);
                    log.info("[catchup node={}] 对端 {} 落后到存档覆盖范围(peerLast={} snapshotIndex={})，下一轮发完整存档",
                            nodeId, peer, peerLast, snapshotIndex());
                } else {
                    // 普通冲突/落后：回退到对端实际位点之后，逐轮补增量
                    nextIndex.put(peer, Math.max(storage.firstIndex(), peerLast + 1L));
                }
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

    private boolean sendSnapshotToPeer(String peer) {
        Snapshot s = storage.loadSnapshot().orElse(null);
        if (s == null) {
            // 本地没有存档却要求对端从更早位点开始：对端已超出本节点的可恢复窗口
            log.warn("[catchup node={}] 对端 {} 需要的历史早于本节点保留窗口(first={})且无存档可发",
                    nodeId, peer, storage.firstIndex());
            return false;
        }
        Optional<InstallSnapshotResponse> reply = transport.sendInstallSnapshot(
                nodeId, peer, new InstallSnapshotRequest(currentTerm, nodeId, s));
        if (reply.isEmpty()) {
            return false;
        }
        InstallSnapshotResponse resp = reply.get();
        if (resp.term() > currentTerm) {
            becomeFollower(resp.term(), null);
            return false;
        }
        if (role != NodeRole.LEADER) {
            return false;
        }
        if (!resp.success()) {
            return false;
        }
        matchIndex.put(peer, s.lastIncludedIndex());
        nextIndex.put(peer, s.lastIncludedIndex() + 1L);
        log.info("[catchup node={}] 向 {} 安装存档成功 snapshotIndex={} term={}，随后补增量",
                nodeId, peer, s.lastIncludedIndex(), s.lastIncludedTerm());
        return true;
    }

    public AppendResponse handleAppendEntries(AppendRequest request) {
        if (request.term() < currentTerm) {
            return AppendResponse.mismatch(currentTerm, storage.lastIndex());
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
        long myK = snapshotIndex();

        if (prevIndex < myK) {
            // 领导者拿已被我存档覆盖的位点来当前置：它的存档比我旧，我不能降级
            log.info("[catchup node={}] 拒绝增量: 前置位点 {} 已被本地存档(K={})覆盖 (依据: 超出可恢复窗口，不接受更旧的历史)",
                    nodeId, prevIndex, myK);
            return AppendResponse.mismatch(currentTerm, storage.lastIndex());
        }
        if (prevIndex == myK && myK > 0L) {
            if (prevTerm != termAt(prevIndex)) {
                throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                        "存档边界任期不一致: snapshotIndex=" + myK
                                + " snapshotTerm=" + termAt(prevIndex)
                                + " requestPrevTerm=" + prevTerm);
            }
        } else if (prevIndex > myK) {
            LogEntry prev = storage.loadEntry(prevIndex);
            if (prev == null && prevIndex <= storage.lastIndex()) {
                // 前置位点落在存档之后、本地最后位点之前却没有条目：
                // 本地历史出现无法跨越的空洞（离线期间区段已被物理回收）
                throw new CoordinationException(CoordinationErrorCode.BEYOND_RECOVERY_WINDOW,
                        "拒绝增量: 前置位点 " + prevIndex + " 处本地历史存在空洞（first="
                                + storage.firstIndex() + ", last=" + storage.lastIndex()
                                + ", snapshotK=" + myK + "），禁止从中间位点续传");
            }
            if (prev == null) {
                // 本地比领导者短：回退一位继续探，逐轮补增量
                return AppendResponse.mismatch(currentTerm, storage.lastIndex());
            }
            if (prev.term() != prevTerm) {
                log.debug("[append node={}] 拒绝: prevLog[{}/term={}] 任期冲突",
                        nodeId, prevIndex, prevTerm);
                // 前置检查不过：不动本地日志，回退领导者的 nextIndex；
                // 真正的冲突条目会在 incoming 逐条处理时按标准 Raft 截断覆盖。
                return AppendResponse.mismatch(currentTerm, storage.lastIndex());
            }
        }

        long baseIndex = prevIndex;
        for (LogEntry incoming : request.entries()) {
            long idx = baseIndex + 1L;
            if (idx <= myK) {
                // 该条目已被存档覆盖：不可能与存档不同，跳过
                baseIndex = idx;
                continue;
            }
            LogEntry existing = storage.loadEntry(idx);
            if (existing == null) {
                if (idx != storage.lastIndex() + 1L) {
                    throw new CoordinationException(CoordinationErrorCode.BEYOND_RECOVERY_WINDOW,
                            "拒绝在历史空洞上追加: idx=" + idx + " lastIndex=" + storage.lastIndex()
                                    + " firstIndex=" + storage.firstIndex());
                }
                storage.appendEntry(new LogEntry(idx, incoming.term(),
                        incoming.idempotencyKey(), incoming.value()));
            } else if (existing.term() != incoming.term()) {
                // 冲突：覆盖未提交的本地尾巴（标准 Raft 截断），随后追加领导者条目
                storage.truncateSuffixFrom(idx);
                storage.appendEntry(new LogEntry(idx, incoming.term(),
                        incoming.idempotencyKey(), incoming.value()));
            }
            // existing 且 term 相同：幂等忽略（重复投递）
            baseIndex = idx;
        }
        if (request.leaderCommit() > commitIndex) {
            commitIndex = Math.min(request.leaderCommit(), storage.lastIndex());
            persistAppliedCheckpoint();
        }
        applyCommitted();
        long matched = baseIndex;
        return AppendResponse.ok(currentTerm, matched);
    }

    /** 接收领导者的完整存档（追赶）。 */
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        if (request.term() < currentTerm) {
            return new InstallSnapshotResponse(currentTerm, false, lastApplied);
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), request.leaderId());
        } else {
            role = NodeRole.FOLLOWER;
            leaderId = request.leaderId();
            resetElectionTimer();
        }
        Snapshot incoming = request.snapshot();
        long myK = snapshotIndex();
        if (incoming.lastIncludedIndex() < myK) {
            // 显式挡回：不允许用更旧的存档覆盖较新的状态，也不悄悄从中间位点续传
            throw new CoordinationException(CoordinationErrorCode.BEYOND_RECOVERY_WINDOW,
                    "拒绝安装更旧的存档: incomingK=" + incoming.lastIncludedIndex()
                            + " localK=" + myK + "（已超出该存档的可恢复窗口）");
        }
        if (incoming.lastIncludedIndex() == myK) {
            if (incoming.lastIncludedTerm() != storage.loadSnapshot()
                    .map(Snapshot::lastIncludedTerm).orElse(-1L)) {
                throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                        "同位点存档任期不一致: K=" + myK
                                + " incomingTerm=" + incoming.lastIncludedTerm());
            }
            // 幂等：已安装同位点存档，直接确认
            return new InstallSnapshotResponse(currentTerm, true, myK);
        }

        // 安装顺序：先落盘存档（会物理删除其覆盖范围的旧日志，覆盖范围外的尾巴保留，
        // 若与存档边界恰好续接且任期一致则仍可用，否则由后续 AppendEntries 冲突截断处理）
        storage.installSnapshot(incoming);
        stateMachine.installSnapshot(incoming);
        lastApplied = incoming.lastIncludedIndex();
        commitIndex = Math.max(commitIndex, incoming.lastIncludedIndex());
        persistAppliedCheckpoint();
        if (snapshotListener != null) {
            snapshotListener.accept(incoming);
        }
        log.info("[catchup node={}] 安装历史存档 snapshotIndex={} term={} 状态键={}，等待后续增量",
                nodeId, incoming.lastIncludedIndex(), incoming.lastIncludedTerm(),
                incoming.state().size());
        return new InstallSnapshotResponse(currentTerm, true, incoming.lastIncludedIndex());
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
            if (appliedListener != null) {
                appliedListener.accept(entry);
            }
            log.info("[apply node={}] 应用日志 index={} term={} idempotencyKey={} value={}",
                    nodeId, idx, entry.term(), entry.idempotencyKey(), appliedValue);
        }
        // 应用之后检查历史压缩：只压缩已经落定生效的部分
        maybeCompactHistory();
    }

    private void maybeCompactHistory() {
        if (compactionThresholdEntries <= 0L) {
            return;
        }
        LogCompactor compactor = new LogCompactor(storage, stateMachine, compactionThresholdEntries);
        compactor.maybeCompact(lastApplied).ifPresent(k -> {
            // 存档落盘后内存位点不变（存档位点 == 已应用位点的子集）；
            // 内存去重表的存档装载由 snapshotListener 之外的 restore 保证，
            // 实时路径下去重表本来就已包含这些键，无需额外动作。
            log.debug("[compaction node={}] 自动压缩到存档位点={}", nodeId, k);
        });
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

        // 立即同步复制一轮：多数派 ACK 则续约+提交；否则按失去多数派处理。
        // 这一轮心跳同时会为落后节点安装存档/补增量，追赶与写入互不阻塞。
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
        // 直接返回内存中的条目：该位点可能在本轮提交后立即被压缩进存档，磁盘 loadEntry 会返回 null
        return entry;
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

    private long snapshotIndex() {
        return storage.loadSnapshot().map(Snapshot::lastIncludedIndex).orElse(0L);
    }

    /** 取位点对应的任期：先查保留日志，查不到再查存档边界。 */
    private long termAt(long index) {
        if (index <= 0L) {
            return 0L;
        }
        LogEntry e = storage.loadEntry(index);
        if (e != null) {
            return e.term();
        }
        return storage.loadSnapshot()
                .filter(s -> s.lastIncludedIndex() == index)
                .map(Snapshot::lastIncludedTerm)
                .orElse(0L);
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

    /** 本地历史压缩位点（无存档时为 0）。 */
    public long getSnapshotIndex() {
        return snapshotIndex();
    }

    /** 本地保留日志的最早位点（压缩后 > 1）。 */
    public long getFirstLogIndex() {
        return storage.firstIndex();
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
        return storage.loadEntries(storage.firstIndex());
    }
}
