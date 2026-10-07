package com.github.lisama569.distributedcoordinationservicejava.cluster;

import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.core.SimClock;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Checkpoint;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import com.github.lisama569.distributedcoordinationservicejava.snapshot.Snapshot;
import com.github.lisama569.distributedcoordinationservicejava.statemachine.KvStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
 *
 * <h3>历史压缩（Log Compaction）</h3>
 * <ul>
 *   <li>每当已应用位点相对存档位点推进了 {@code compactionThresholdEntries} 条，就把
 *       <b>截至已应用位点</b>的历史压成一份 {@link Snapshot}：含状态机全量数据与全部
 *       已生效幂等写索引，随后删除被覆盖的旧日志前缀；</li>
 *   <li>只压缩已提交应用的内容，未确认生效的日志永远进不了存档，已承诺生效的写入
 *       一条都不会丢（幂等索引一并进存档）；</li>
 *   <li>崩溃恢复 / 全新节点：先安装存档把状态一次性带到存档位点，再补存档之后的增量，
 *       结果与从头完整重放严格一致。</li>
 * </ul>
 *
 * <h3>滞后节点追赶（Catch-up）</h3>
 * <ul>
 *   <li>AppendEntries 拒绝时携带冲突任期/位点提示，领导者据此<b>一轮</b>把 nextIndex
 *       跳到真正分叉处（而不是每轮只回退一格）：纯滞后一轮补齐整段增量、任期分叉两轮
 *       对齐，追赶轮数基本不随落后多少而变长；领导者发现对端所需位点已被自己压缩进
 *       存档时改推 InstallSnapshot，对端装完存档后继续走 AppendEntries 补增量，
 *       空盘（重置/全新）节点在代次识别下一轮即收到存档；</li>
 *   <li>追赶期间领导者照常接受新写入，多数派由健康副本凑齐，补数据不阻塞提交；
 *       存档整体安装 + 增量按位点严格续接，同一次写不会被算两遍；</li>
 *   <li>只有<b>空盘的新/重置节点</b>（或已持有同一存档的幂等重传）才允许整体安装存档；
 *       自带旧历史、且所需增量已被集群压缩掉的掉队节点会被以
 *       {@link CoordinationErrorCode#OUT_OF_RETENTION_WINDOW} 明确挡回，
 *       绝不允许覆盖旧历史、也不允许从某个中间位置悄悄续写。该类节点必须显式重置本地
 *       磁盘后，才能以"存档 + 增量"重新加入。</li>
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
    /** 已应用位点相对存档位点每推进多少条触发一次压缩；&lt;=0 表示关闭压缩 */
    private final long compactionThresholdEntries;
    /** 每个节点固定的选举超时扰动，避免同时超时导致分票 */
    private final long electionJitter;

    // ---- 持久化状态（内存镜像，权威数据在 storage） ----
    private long currentTerm;
    private String votedFor;

    // ---- 存档位点（与磁盘存档一致；无存档时为 0） ----
    private long snapIndex;
    private long snapTerm;

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
    /** 已被告知"超出可恢复窗口、必须重置"的对端（重置后下一轮存档推送自动清除） */
    private final Map<String, String> blockedCatchUp = new LinkedHashMap<>();
    /** 领导者记录的各对端成员代次：变化即说明对端已清空历史重加入，旧复制进度作废 */
    private final Map<String, Long> peerIncarnations = new HashMap<>();
    private long leaseExpireAtMillis = Long.MIN_VALUE;
    private long nextHeartbeatAt;

    /** 已生效幂等写索引（权威）：idempotencyKey -> (term,index,value)，压缩时整体进存档 */
    private final Map<String, Snapshot.IdempotentRecord> appliedIdempotent = new LinkedHashMap<>();

    public RaftNode(String nodeId, SimClock clock, DurableStorage storage,
                    KvStateMachine stateMachine, ClusterTransport transport,
                    long electionTimeoutMillis, long heartbeatIntervalMillis,
                    long leaseTtlMillis) {
        this(nodeId, clock, storage, stateMachine, transport,
                electionTimeoutMillis, heartbeatIntervalMillis, leaseTtlMillis,
                ClusterSimulator.DEFAULT_COMPACTION_THRESHOLD_ENTRIES);
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

    // ==================== 崩溃恢复 ====================

    private void recover() {
        Checkpoint cp = storage.loadCheckpoint();
        this.currentTerm = cp.currentTerm();
        this.votedFor = cp.votedFor();

        // 第一步：先安装存档，把状态机与幂等索引一次性带到存档位点
        Snapshot snapshot = storage.loadSnapshot();
        this.snapIndex = snapshot.lastIncludedIndex();
        this.snapTerm = snapshot.lastIncludedTerm();
        if (snapIndex > 0L) {
            stateMachine.installSnapshot(snapIndex, snapTerm, snapshot.data());
            appliedIdempotent.clear();
            appliedIdempotent.putAll(snapshot.idempotent());
            log.info("[recover node={}] 加载历史存档: snapIndex={} snapTerm={} 存档幂等记录={} 条 (依据: 存档等价于重放 1..snapIndex)",
                    nodeId, snapIndex, snapTerm, snapshot.idempotent().size());
        }

        // 第二步：补存档之后、检查点之前（已提交应用）的增量日志
        this.commitIndex = cp.appliedIndex();
        this.lastApplied = snapIndex;
        for (long i = snapIndex + 1L; i <= cp.appliedIndex(); i++) {
            LogEntry e = storage.loadEntry(i);
            if (e == null) {
                throw new CoordinationException(CoordinationErrorCode.INTERNAL,
                        "恢复失败: 存档之后缺少已提交日志 index=" + i + " snapIndex=" + snapIndex);
            }
            stateMachine.apply(i, keyOf(e), e.value());
            rememberIdempotent(e);
            lastApplied = i;
        }
        // 兼容旧检查点（无存档）：appliedIndex 即权威应用位点
        if (lastApplied < cp.appliedIndex()) {
            lastApplied = cp.appliedIndex();
        }
        this.role = NodeRole.FOLLOWER;
        this.leaderId = null;
        this.leaseExpireAtMillis = Long.MIN_VALUE;
        log.info("[recover node={}] 恢复完成: term={} snapIndex={} lastIndex={} appliedIndex={}",
                nodeId, currentTerm, snapIndex, storage.lastIndex(), lastApplied);
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
        blockedCatchUp.clear();
        // 注意：peerIncarnations 不清空——代次记忆必须跨任期保留，才能识别
        // "同名但已清空磁盘重加入"的对端；同代次对端一律按乐观位点首次探测。
        for (String peer : transport.nodeIds()) {
            if (!peer.equals(nodeId)) {
                nextIndex.put(peer, localLastIndex() + 1L);
                matchIndex.put(peer, snapIndex);
            }
        }
        // 当选即发一轮心跳：既宣告领导权，也尝试立刻获得多数派 ACK 以建立租约
        broadcastHeartbeats();
        nextHeartbeatAt = clock.nowMillis() + heartbeatIntervalMillis;
        log.info("[elect node={}] 成为领导者 term={} 集群规模={} snapIndex={}",
                nodeId, currentTerm, transport.nodeIds().size(), snapIndex);
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

    // ==================== 心跳、快照追赶与复制 ====================

    private void broadcastHeartbeats() {
        int total = transport.nodeIds().size();
        int acks = 1; // 自己
        for (String peer : transport.nodeIds()) {
            if (peer.equals(nodeId)) {
                continue;
            }
            // 新加入或"重置后重加入"的对端：磁盘已清空、旧 nextIndex 作废。
            // 用成员代次识别"同名但已重置"的对端，丢弃旧复制进度，从存档位点开始同步。
            long incarnation = transport.incarnationOf(peer);
            Long knownIncarnation = peerIncarnations.get(peer);
            if (knownIncarnation == null) {
                // 首次见到该代次的对端：先按乐观位点（领导者日志尾之后）尝试——
                // 健康对端一轮心跳即可 ACK；落后/分叉对端会带冲突提示拒绝，下一轮直达。
                nextIndex.put(peer, localLastIndex() + 1L);
                matchIndex.put(peer, snapIndex);
                peerIncarnations.put(peer, incarnation);
            } else if (knownIncarnation != incarnation) {
                // 同名节点代次变化 = 已显式清空本地历史重加入：旧进度作废，
                // 本地已有存档时 nextIndex 直接对准存档位点，本轮就推 InstallSnapshot；
                // 无存档时从首条日志开始。
                nextIndex.put(peer, snapIndex > 0L ? snapIndex : 1L);
                matchIndex.put(peer, snapIndex);
                blockedCatchUp.remove(peer);
                peerIncarnations.put(peer, incarnation);
                log.info("[catchup node={}] 对端 {} 已重置重加入(代次 {} -> {})，从存档位点 snapIndex={} 开始同步",
                        nodeId, peer, knownIncarnation, incarnation, snapIndex);
            }
            // 已被明确挡在可恢复窗口之外的对端：在显式重置（代次变化）前不再重复推存档，
            // 既不假装追平，也不拿无效复制流量干扰；同时不计入 ACK。
            if (blockedCatchUp.containsKey(peer)) {
                continue;
            }
            long ni = nextIndex.getOrDefault(peer, localLastIndex() + 1L);
            List<LogEntry> batch = storage.loadEntries(ni);
            long prevIndexForPeer = ni - 1L;
            long prevTermForPeer = termAt(prevIndexForPeer);
            // 何时必须推存档：对端需要的这批增量，我本地已经一条都不剩了
            // （ni 已被我压缩进存档）。注意 prevIndex 恰好等于我存档位点时是合法的
            // AppendEntries 边界（对端装过同一存档，用空/后续批量即可对齐），不能误推快照。
            boolean peerNeedsSnapshot = ni <= snapIndex;
            if (peerNeedsSnapshot) {
                if (sendSnapshot(peer)) {
                    acks++;
                }
                continue;
            }
            AppendRequest request = new AppendRequest(currentTerm, nodeId, prevIndexForPeer,
                    prevTermForPeer, commitIndex, batch);
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
                long matched = prevIndexForPeer + batch.size();
                matchIndex.put(peer, matched);
                nextIndex.put(peer, matched + 1L);
            } else {
                // 对端拒绝：按冲突任期/位点提示一轮跳到分叉处，追赶轮数与落后条数无关。
                long backtrackTo = resolveRejectedNextIndex(resp);
                nextIndex.put(peer, backtrackTo);
                matchIndex.put(peer, Math.min(matchIndex.getOrDefault(peer, snapIndex), backtrackTo - 1L));
                log.info("[catchup node={}] 对端 {} 拒绝增量(对端snap={} 冲突任期={} 冲突位点={})，"
                                + "nextIndex {} -> {} (依据: 冲突任期跳转，至多再经一轮存档/增量即对齐)",
                        nodeId, peer, resp.peerSnapIndex(), resp.conflictTerm(), resp.conflictIndex(),
                        ni, backtrackTo);
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

    /**
     * 领导者向落后对端推送本地存档。
     *
     * @return 对端是否接受（接受/可计入 ACK）；被明确挡回（超出可恢复窗口）时返回 false
     */
    private boolean sendSnapshot(String peer) {
        Snapshot snapshot = storage.loadSnapshot();
        InstallSnapshotRequest request = new InstallSnapshotRequest(currentTerm, nodeId, snapshot);
        Optional<InstallSnapshotResponse> reply = transport.sendInstallSnapshot(nodeId, peer, request);
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
        if (resp.success()) {
            blockedCatchUp.remove(peer);
            matchIndex.put(peer, snapshot.lastIncludedIndex());
            nextIndex.put(peer, snapshot.lastIncludedIndex() + 1L);
            log.info("[catchup node={}] 对端 {} 已安装存档至 snapIndex={}，后续改走增量复制",
                    nodeId, peer, snapshot.lastIncludedIndex());
            return true;
        }
        if (CoordinationErrorCode.OUT_OF_RETENTION_WINDOW.name().equals(resp.reason())) {
            String detail = "对端 " + peer + " 自带旧历史且所需位点 " + (snapIndex + 1L)
                    + " 已超出其可恢复窗口(本地最早可服务位点=" + snapIndex
                    + ")，必须重置本地磁盘后以存档+增量重新加入";
            blockedCatchUp.put(peer, detail);
            log.warn("[catchup node={}] {}（不覆盖旧历史、不从中间位点续写）",
                    nodeId, detail);
        }
        return false;
    }

    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        if (request.term() < currentTerm) {
            return InstallSnapshotResponse.reject(currentTerm, 0L, CoordinationErrorCode.STALE_TERM.name());
        }
        if (request.term() > currentTerm) {
            becomeFollower(request.term(), request.leaderId());
        } else {
            // 同 term：承认对端领导权
            role = NodeRole.FOLLOWER;
            leaderId = request.leaderId();
            resetElectionTimer();
        }
        Snapshot incoming = request.snapshot();

        if (incoming.lastIncludedIndex() < snapIndex) {
            // 领导者的存档比我本地的还旧：它不可能是合法领导者（日志更旧），明确拒绝
            log.info("[catchup node={}] 拒绝旧存档: incomingSnap={} < localSnap={}",
                    nodeId, incoming.lastIncludedIndex(), snapIndex);
            return InstallSnapshotResponse.reject(currentTerm, snapIndex,
                    CoordinationErrorCode.CHECKPOINT_ROLLBACK.name());
        }
        boolean cleanDisk = snapIndex == 0L && storage.lastIndex() == 0L;
        boolean sameSnapshot = incoming.lastIncludedIndex() == snapIndex;
        // 对端没有"存档覆盖范围之外、会被新存档抹掉"的本地日志时，允许（重新）安装：
        //  - 空盘的新/重置节点（cleanDisk）；
        //  - 幂等重传同一份存档（sameSnapshot）；
        //  - 已装过旧存档、尚无存档之外增量（storage.lastIndex()==snapIndex）：
        //    只是把权威状态更新得更全，属于正常追赶，不是从中间位点续写。
        boolean hasOnlySnapshotState = storage.lastIndex() == snapIndex;
        if (!cleanDisk && !sameSnapshot && !hasOnlySnapshotState) {
            // 本地仍带着存档覆盖范围之外的旧历史，而所需增量已被集群压缩：
            // 不覆盖、不续写，明确挡回，要求显式重置后以存档+增量重新加入。
            log.warn("[catchup node={}] 拒绝整体安装存档: 本地存在存档之外的旧历史 snapIndex={} lastIndex={}，"
                            + "incomingSnap={}（超出可恢复窗口，需显式重置后重加入）",
                    nodeId, snapIndex, storage.lastIndex(), incoming.lastIncludedIndex());
            return InstallSnapshotResponse.reject(currentTerm, snapIndex,
                    CoordinationErrorCode.OUT_OF_RETENTION_WINDOW.name());
        }

        Checkpoint installed = storage.installSnapshot(incoming);
        stateMachine.installSnapshot(incoming.lastIncludedIndex(),
                incoming.lastIncludedTerm(), incoming.data());
        appliedIdempotent.clear();
        appliedIdempotent.putAll(incoming.idempotent());
        this.snapIndex = incoming.lastIncludedIndex();
        this.snapTerm = incoming.lastIncludedTerm();
        this.commitIndex = installed.appliedIndex();
        this.lastApplied = installed.appliedIndex();
        log.info("[catchup node={}] 安装领导者存档 snapIndex={} snapTerm={} 幂等记录={} 条，"
                        + "状态已与从头重放一致，等待位点 {} 之后的增量",
                nodeId, snapIndex, snapTerm, incoming.idempotent().size(), snapIndex + 1L);
        return InstallSnapshotResponse.ok(currentTerm, snapIndex);
    }

    public AppendResponse handleAppendEntries(AppendRequest request) {
        if (request.term() < currentTerm) {
            return AppendResponse.reject(currentTerm, 0L, 0L, snapIndex);
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
            long knownPrevTerm = termAt(prevIndex);
            if (knownPrevTerm == 0L || knownPrevTerm != prevTerm) {
                AppendResponse hint;
                if (prevIndex <= snapIndex) {
                    // 需要的前序位点已被本地存档覆盖：普通 Append 无法服务，提示领导者改推存档。
                    // conflictTerm=0 + 冲突位点=prevIndex(<=领导者snap时它会识别) ；
                    // 同时明确带上本地存档位点。
                    log.debug("[append node={}] 拒绝: prevLog[{}] 已被本地存档覆盖 snapIndex={}",
                            nodeId, prevIndex, snapIndex);
                    hint = AppendResponse.reject(currentTerm, 0L, prevIndex, snapIndex);
                } else {
                    LogEntry conflictEntry = storage.loadEntry(prevIndex);
                    if (conflictEntry == null) {
                        // 该位点缺日志（空洞/我更短）：告知我的日志尾，让领导者一轮跳到我尾部
                        log.debug("[append node={}] 拒绝: prevLog[{}] 缺失，本地日志尾={}",
                                nodeId, prevIndex, storage.lastIndex());
                        hint = AppendResponse.reject(currentTerm, 0L, storage.lastIndex(), snapIndex);
                    } else {
                        // 任期冲突：告知该任期在我本地的首条位点，领导者一轮跳到真正分叉处
                        long conflictTerm = conflictEntry.term();
                        long firstOfTerm = firstIndexOfRetainedTerm(conflictTerm);
                        log.debug("[append node={}] 拒绝: prevLog[{}/term={}] 与本地任期 {} 冲突，任期首条位点={}",
                                nodeId, prevIndex, prevTerm, conflictTerm, firstOfTerm);
                        hint = AppendResponse.reject(currentTerm, conflictTerm, firstOfTerm, snapIndex);
                    }
                }
                return hint;
            }
        }
        long baseIndex = prevIndex;
        for (LogEntry incoming : request.entries()) {
            long idx = baseIndex + 1L;
            LogEntry existing = storage.loadEntry(idx);
            if (existing == null) {
                if (idx < storage.firstRetainedIndex()) {
                    // 该位点已被本地存档覆盖，绝不能再写：交给领导者通过快照对齐
                    log.debug("[append node={}] 拒绝: idx={} 已被本地存档覆盖 snapIndex={}",
                            nodeId, idx, snapIndex);
                    return AppendResponse.reject(currentTerm, 0L, idx, snapIndex);
                }
                if (idx != storage.lastIndex() + 1L) {
                    // 中间存在空洞：说明缺更早的增量（或需要快照），把本地日志尾告诉领导者，
                    // 它一轮即可回跳到正确位点；注意安装存档后 lastIndex()==snapIndex，
                    // 因此首条 snapIndex+1 能正常续上
                    log.debug("[append node={}] 拒绝: idx={} 存在空洞 lastIndex={} snapIndex={}",
                            nodeId, idx, storage.lastIndex(), snapIndex);
                    return AppendResponse.reject(currentTerm, 0L, storage.lastIndex(), snapIndex);
                }
                storage.appendEntry(new LogEntry(idx, incoming.term(),
                        incoming.idempotencyKey(), incoming.value()));
            } else if (existing.term() != incoming.term()) {
                // 任期冲突：删掉该位点及之后的后缀，用领导者日志覆盖（标准 Raft 冲突处理）
                log.info("[append node={}] index={} 任期冲突 existing={} incoming={}，截断后缀并覆盖",
                        nodeId, idx, existing.term(), incoming.term());
                storage.truncateFrom(idx);
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
        long matched = prevIndex + request.entries().size();
        return AppendResponse.ok(currentTerm, matched);
    }

    /** 在存档之后保留的增量日志中，查找指定任期第一条日志的位点。 */
    private long firstIndexOfRetainedTerm(long term) {
        for (long i = snapIndex + 1L; i <= storage.lastIndex(); i++) {
            LogEntry e = storage.loadEntry(i);
            if (e != null && e.term() == term) {
                return i;
            }
        }
        return storage.lastIndex();
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
            rememberIdempotent(entry);
            lastApplied = idx;
            persistAppliedCheckpoint();
            log.info("[apply node={}] 应用日志 index={} term={} idempotencyKey={} value={}",
                    nodeId, idx, entry.term(), entry.idempotencyKey(), appliedValue);
        }
        maybeCompact();
    }

    private void rememberIdempotent(LogEntry entry) {
        if (entry.idempotencyKey() != null) {
            appliedIdempotent.putIfAbsent(entry.idempotencyKey(),
                    new Snapshot.IdempotentRecord(entry.term(), entry.index(), entry.value()));
        }
    }

    // ==================== 历史压缩 ====================

    /**
     * 规模阈值触发：当存档之外堆积的已应用增量达到 {@code compactionThresholdEntries} 条时，
     * 把更老的历史压进存档，并始终在存档之外保留 threshold 条最新增量
     * （因此正在提交/返回的当条绝不会被截断，落后在窗口内的节点仍能用增量追上）。
     * 存档来源只有"状态机当前全量数据 + 已生效幂等索引"，二者都只由已提交应用的日志产生。
     */
    private void maybeCompact() {
        if (compactionThresholdEntries <= 0L) {
            return;
        }
        // 存档之外保留 threshold 条增量：压缩到 lastApplied - threshold
        long compactThrough = lastApplied - compactionThresholdEntries;
        if (compactThrough <= snapIndex) {
            return;
        }
        long compactTerm = termAt(compactThrough);
        Map<String, String> data = stateMachine.snapshot();
        Map<String, Snapshot.IdempotentRecord> idempotent = Map.copyOf(appliedIdempotent);
        Snapshot snapshot = new Snapshot(compactThrough, compactTerm, currentTerm,
                Map.copyOf(data), idempotent);
        storage.saveSnapshot(snapshot);
        storage.compactPrefixThrough(compactThrough);
        long removed = compactThrough - snapIndex;
        this.snapIndex = compactThrough;
        this.snapTerm = compactTerm;
        log.info("[compact node={}] 历史压缩完成: 新存档位点 snapIndex={} snapTerm={}，"
                        + "删除已压缩日志 {} 条，存档含 K/V {} 项、幂等记录 {} 条，保留增量 {} 条 "
                        + "(依据: 仅压缩已提交应用位点 1..{})",
                nodeId, snapIndex, snapTerm, removed, data.size(), idempotent.size(),
                storage.lastIndex() - snapIndex, compactThrough);
    }

    // ==================== 写入 ====================

    /**
     * 领导者接收写请求：校验身份与租约 -> 本地持久化 -> 复制到多数派 -> 提交应用。
     * 复制过程中若有对端在靠存档追赶，不阻塞本写入：多数派由其余健康副本凑齐。
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

    /**
     * 依据对端拒绝时的冲突任期/位点提示，一步算出下一轮应当使用的 nextIndex。
     *
     * <ul>
     *   <li>所需位点我已压进存档（对端 conflictTerm=0 且其冲突位点不晚于我的存档位点，
     *       或跳转目标落到存档覆盖范围内）：返回 snapIndex，下一轮直接 InstallSnapshot；</li>
     *   <li>冲突任期在我的存档之后且本地仍有该任期：跳到该任期最后一条位点（标准 Raft
     *       XTerm 优化的保守取法，保证不会越过我自己的分叉点），下一轮一次对齐；</li>
     *   <li>冲突任期比我保留日志全部任期都新（对端多出我没有的任期）：取它提示的
     *       conflictIndex+1，交由 Raft 冲突覆盖处理；</li>
     *   <li>纯缺日志（conflictTerm=0）：取对端日志尾的下一位点，最多 snapIndex+1。</li>
     * </ul>
     */
    private long resolveRejectedNextIndex(AppendResponse resp) {
        long peerConflictTerm = resp.conflictTerm();
        long peerConflictIndex = resp.conflictIndex();
        // 对端需要的前序位点已经被我压缩进存档：只能靠存档补齐
        if (peerConflictIndex > 0L && peerConflictIndex <= snapIndex) {
            return snapIndex;
        }
        long firstRetained = snapIndex + 1L;
        long localLast = localLastIndex();
        if (localLast < firstRetained) {
            // 本地存档之外没有任何增量可发：只能推存档
            return snapIndex;
        }
        long retainedFirstTerm = termAt(firstRetained);
        if (peerConflictTerm == 0L) {
            // 对端缺日志（空洞/更短/空盘）：
            //  - 它的日志尾还在我存档覆盖范围内（含空盘 conflictIndex=0）-> 增量没有共同
            //    前缀可接，只能改推存档；
            //  - 否则从它日志尾的下一条开始发增量。
            if (peerConflictIndex < firstRetained) {
                return snapIndex;
            }
            return peerConflictIndex + 1L;
        }
        if (peerConflictTerm < retainedFirstTerm) {
            // 分叉任期整体已被我压进存档覆盖范围：增量无法调和，改推存档
            return snapIndex;
        }
        Long lastIndexOfTerm = lastIndexOfRetainedTerm(peerConflictTerm);
        if (lastIndexOfTerm != null) {
            return lastIndexOfTerm + 1L;
        }
        // 我保留的日志里没有该任期（对端在更新任期上有我没有的条目）
        long target = Math.max(firstRetained, peerConflictIndex + 1L);
        return target;
    }

    /** 在存档之后仍保留的增量日志中，查找指定任期的最后一条日志位点；不存在返回 null。 */
    private Long lastIndexOfRetainedTerm(long term) {
        long found = -1L;
        for (long i = snapIndex + 1L; i <= localLastIndex(); i++) {
            LogEntry e = storage.loadEntry(i);
            if (e == null) {
                break;
            }
            if (e.term() == term) {
                found = i;
            } else if (e.term() > term) {
                break;
            }
        }
        return found < 0L ? null : found;
    }

    /**
     * 指定位点的任期：先看增量日志；位点已被存档覆盖（index &lt;= snapIndex）时，
     * 该范围属于已提交历史，统一回落到存档任期（推进提交位点只需比较旧 term 与当前 term）；
     * 位点在存档之后但日志缺失返回 0。
     */
    private long termAt(long index) {
        if (index == 0L) {
            return 0L;
        }
        if (index <= snapIndex) {
            return snapTerm;
        }
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

    /** 当前存档覆盖到位点（无存档返回 0）：更早的位点已不再保留原始日志。 */
    public long getSnapshotIndex() {
        return snapIndex;
    }

    public long getSnapshotTerm() {
        return snapTerm;
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

    /**
     * 权威幂等索引查询：某幂等键是否已在本节点生效（无论来自原始日志还是历史存档）。
     * 协调层据此保证压缩、换主、追赶后的重试都只产生一次写入效果。
     */
    public Snapshot.IdempotentRecord findAppliedIdempotent(String idempotencyKey) {
        return appliedIdempotent.get(idempotencyKey);
    }

    public String readState(String key) {
        return stateMachine.read(key);
    }

    /** 当前被本节点（领导者视角）挡在可恢复窗口之外、必须重置后重加入的对端及原因。 */
    public Map<String, String> blockedCatchUpPeers() {
        return Map.copyOf(blockedCatchUp);
    }

    public List<LogEntry> logView() {
        return storage.loadEntries(snapIndex + 1L);
    }

    /** 存档之外仍保留原始日志的条数：持续压缩下该值应稳定在阈值附近，不再随写入无限增长。 */
    public int retainedLogSize() {
        return storage.loadEntries(snapIndex + 1L).size();
    }

    /** 真正产生写入效果的 apply 次数（每个位点至多一次）。 */
    public long effectiveApplyCount() {
        return stateMachine.effectiveApplyCount();
    }

    /** 同一位点被重复 apply 的次数：追赶/恢复/重传路径下应为 0（或仅崩溃重放预期）。 */
    public long replayApplyCount() {
        return stateMachine.replayApplyCount();
    }

    public static UnsupportedOperationException todo() {
        return new UnsupportedOperationException("尚未实现");
    }
}
