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
 *   <li>领导者发现对端 {@code nextIndex} 已落到自己存档位点之前，改推 InstallSnapshot，
 *       对端装完存档后继续走 AppendEntries 补增量，期间领导者照常接受新写入；</li>
 *   <li><b>快速对齐</b>：AppendEntries 被拒时跟随者回传冲突提示（日志太短 /
 *       冲突任期首个下标 / 需走快照），领导者据此把 {@code nextIndex} 一次跳到位、
 *       下一轮整批补齐——无论落后多少条、中间换没换过主，都只需少数几轮心跳，
 *       而不是逐格回退、落后 N 条拖 N 轮；</li>
 *   <li>候选者拉票失败会额外退避一个选举超时，避免"日志旧但扰动小"的节点在确定性
 *       扰动下反复抢先超时、让多数派永远完不成换届（选举活锁）；</li>
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
        } else {
            // 拉票失败（分裂票或日志不够新被拒）：额外退避一个选举超时。
            // 否则在确定性扰动下，"日志旧但扰动小"的节点会永远抢先超时、
            // 反复抬高任期并重置他人计时器，使多数派永远完不成换届（选举活锁）；
            // 退避后日志更新的节点得以先超时当选，再用心跳压制落后候选者。
            electionDeadline = clock.nowMillis() + 2L * (electionTimeoutMillis + electionJitter);
            log.info("[elect node={}] term={} 拉票失败，退避至 {} 后再试",
                    nodeId, currentTerm, electionDeadline);
        }
    }

    private void becomeLeader() {
        role = NodeRole.LEADER;
        leaderId = nodeId;
        nextIndex.clear();
        matchIndex.clear();
        blockedCatchUp.clear();
        peerIncarnations.clear();
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
            if (knownIncarnation == null || knownIncarnation != incarnation) {
                nextIndex.put(peer, snapIndex + 1L);
                matchIndex.put(peer, snapIndex);
                blockedCatchUp.remove(peer);
                peerIncarnations.put(peer, incarnation);
                log.info("[catchup node={}] 发现（重新）加入的对端 {} (代次={})，从存档位点 snapIndex={} 开始同步",
                        nodeId, peer, incarnation, snapIndex);
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
                // 对端日志落后或冲突：按对端回传的冲突提示把 nextIndex 一次性跳到位
                // （不再每轮只回退一格），落后多少都只需少数几轮即可对齐；
                // 下一轮若连 prevLog 都无法从本地历史解析，会自动改走 InstallSnapshot。
                long jumped = nextIndexFromConflictHint(resp, ni);
                nextIndex.put(peer, jumped);
                log.info("[catchup node={}] 对端 {} 日志不一致: 按冲突提示 nextIndex {} -> {} "
                                + "(conflictTerm={} conflictIndex={})，下一轮整批补齐",
                        nodeId, peer, ni, jumped, resp.conflictTerm(), resp.conflictIndex());
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

    /**
     * 按对端回传的冲突提示计算下一轮 nextIndex（Raft 冲突快速回退）：
     * <ul>
     *   <li>conflictTerm &lt; 0：对端所需位点已被其存档覆盖，回到位点 1 ——
     *       下一轮必然触发 InstallSnapshot（或本地无存档时全量对齐）；</li>
     *   <li>conflictTerm = 0：对端日志太短，直接跳到对端 lastIndex+1；</li>
     *   <li>conflictTerm &gt; 0：若本地持有该任期条目则跳到其最后一条之后，
     *       否则跳到对端给出的冲突段首个下标。</li>
     * </ul>
     * 提示只让 nextIndex 回退、绝不前进（钳制在 currentNext-1 以内），因此不会发散。
     */
    private long nextIndexFromConflictHint(AppendResponse resp, long currentNext) {
        long hint;
        if (resp.conflictTerm() < 0L) {
            hint = 1L;
        } else if (resp.conflictTerm() == 0L) {
            hint = resp.conflictIndex();
        } else {
            long lastOfTerm = lastIndexOfTerm(resp.conflictTerm());
            hint = lastOfTerm > 0L ? lastOfTerm + 1L : resp.conflictIndex();
        }
        return Math.max(1L, Math.min(hint, currentNext - 1L));
    }

    /**
     * 本地日志中指定任期的最后一个下标（不存在返回 0）。日志内任期单调非降，二分查找。
     * 该任期已被本地存档覆盖时保守返回存档位点（等价于"从存档边界之后开始对齐"）。
     */
    private long lastIndexOfTerm(long term) {
        if (term <= 0L) {
            return 0L;
        }
        if (snapIndex > 0L && term <= snapTerm) {
            return term == snapTerm ? snapIndex : 0L;
        }
        long lo = snapIndex + 1L, hi = storage.lastIndex(), ans = 0L;
        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            long t = termAt(mid);
            if (t == term) {
                ans = mid;
                lo = mid + 1L;
            } else if (t == 0L || t > term) {
                hi = mid - 1L;
            } else {
                lo = mid + 1L;
            }
        }
        return ans;
    }

    /**
     * 本地日志中指定任期的首个下标（不存在返回 0）。日志内任期单调非降，二分查找。
     * 该任期即存档任期时，首个可见下标即存档位点本身。
     */
    private long firstIndexOfTerm(long term) {
        if (term <= 0L) {
            return 0L;
        }
        if (snapIndex > 0L && term <= snapTerm) {
            return term == snapTerm ? snapIndex : 0L;
        }
        long lo = snapIndex + 1L, hi = storage.lastIndex(), ans = 0L;
        while (lo <= hi) {
            long mid = (lo + hi) >>> 1;
            long t = termAt(mid);
            if (t == term) {
                ans = mid;
                hi = mid - 1L;
            } else if (t == 0L || t > term) {
                hi = mid - 1L;
            } else {
                lo = mid + 1L;
            }
        }
        return ans;
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
            long knownPrevTerm = termAt(prevIndex);
            if (knownPrevTerm == 0L) {
                // prevLog 超出本地日志范围：本地日志太短，提示领导者直接跳到 lastIndex+1
                log.debug("[append node={}] 拒绝: prevLog[{}] 超出本地日志 lastIndex={}，"
                        + "回传提示 nextIndex={}", nodeId, prevIndex,
                        storage.lastIndex(), storage.lastIndex() + 1L);
                return AppendResponse.rejectLogTooShort(currentTerm, storage.lastIndex());
            }
            if (knownPrevTerm != prevTerm) {
                if (prevIndex <= snapIndex) {
                    // 需要的前序位点已被本地存档覆盖且任期对不上：普通 Append 无法服务，
                    // 提示领导者改推存档
                    log.debug("[append node={}] 拒绝: prevLog[{}] 已被本地存档覆盖 snapIndex={}",
                            nodeId, prevIndex, snapIndex);
                    return AppendResponse.rejectNeedsSnapshot(currentTerm, snapIndex);
                }
                // prevLog 任期冲突：回传冲突任期及其在本地日志中的首个下标
                long firstIdx = firstIndexOfTerm(knownPrevTerm);
                log.debug("[append node={}] 拒绝: prevLog[{}/term={}] 与本地 term={} 冲突，"
                        + "回传提示 conflictIndex={}", nodeId, prevIndex, prevTerm,
                        knownPrevTerm, firstIdx);
                return AppendResponse.rejectTermConflict(currentTerm, knownPrevTerm, firstIdx);
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
                    return AppendResponse.rejectNeedsSnapshot(currentTerm, snapIndex);
                }
                if (idx != storage.lastIndex() + 1L) {
                    // 中间存在空洞：说明缺更早的增量（或需要快照），拒绝并提示领导者
                    // 直接跳到本地日志末尾之后（一轮到位，不再逐格回退）；
                    // 注意安装存档后 lastIndex()==snapIndex，因此首条 snapIndex+1 能正常续上
                    log.debug("[append node={}] 拒绝: idx={} 存在空洞 lastIndex={} snapIndex={}",
                            nodeId, idx, storage.lastIndex(), snapIndex);
                    return AppendResponse.rejectLogTooShort(currentTerm, storage.lastIndex());
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

    public static UnsupportedOperationException todo() {
        return new UnsupportedOperationException("尚未实现");
    }
}
