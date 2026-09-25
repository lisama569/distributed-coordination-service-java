package com.github.lisama569.distributedcoordinationservicejava.coord.raft;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.NodeRole;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.recovery.Checkpoint;
import com.github.lisama569.distributedcoordinationservicejava.coord.recovery.CheckpointStore;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.Network;
import com.github.lisama569.distributedcoordinationservicejava.coord.statemachine.Outcome;
import com.github.lisama569.distributedcoordinationservicejava.coord.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单个 Raft 节点：多数派选举 + 领导者租约 + 栅栏令牌 + 幂等应用 + 崩溃恢复。
 *
 * <p>选举安全：候选者/投票者都在“已知上一任领导者的租约到期时间”之后才行动，
 * 领导者一旦延长租约立即在同一同步调用中广播新的租约到期提示（模拟网络即时投递，
 * 不存在提示滞后窗口），因此同一任期不可能出现两个被承认的领导者，旧任期也不会
 * 覆盖新任期。</p>
 *
 * <p>领导者租约：领导者每次收到多数派心跳应答才延长租约；失去多数派后租约必然
 * 到期，{@link #propose} 在租约失效时以 {@link RejectReason#LEADER_LEASE_EXPIRED}
 * 拒绝写入，节点同时主动降级。</p>
 *
 * <p>时间完全由外部模拟时钟以 {@link #tick(long)} 推进，消息即时投递，全程单线程，
 * 行为确定可复现。</p>
 */
public class RaftNode implements Network.Endpoint {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final List<String> allNodeIds;
    private final Network network;
    private final ClusterControl control;
    private final CheckpointStore store;
    private final SimTimers timers;
    private final int quorum;

    // ---- Raft 持久状态 ----
    private long currentTerm;
    private String votedFor;
    private final List<LogEntry> raftLog = new ArrayList<>();

    // ---- Raft 易失状态 ----
    private NodeRole role = NodeRole.FOLLOWER;
    private long commitIndex;
    private long lastApplied;

    // ---- 领导者租约 ----
    private long leaseUntilMillis;
    private long lastHeartbeatSent = Long.MIN_VALUE;
    private final Map<String, Long> peerLastAckAt = new HashMap<>();

    // ---- 领导者复制进度 ----
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    // ---- 候选者得票 ----
    private final java.util.Set<String> votesGranted = new java.util.HashSet<>();

    // ---- 选举计时 / 旧领导者租约提示 ----
    private long electionDeadline;
    private long lastLeaderLeaseHintUntil;
    private int electionAttempt;

    // ---- 栅栏序号（任期内单调递增）----
    private long lastFenceSequence;

    private final StateMachine stateMachine = new StateMachine();
    private final Map<String, Outcome> idempotentResults = new HashMap<>();

    public RaftNode(String nodeId, List<String> allNodeIds, Network network,
                    ClusterControl control, CheckpointStore store, SimTimers timers) {
        this.nodeId = nodeId;
        this.allNodeIds = List.copyOf(allNodeIds);
        this.network = network;
        this.control = control;
        this.store = store;
        this.timers = timers;
        this.quorum = allNodeIds.size() / 2 + 1;
        this.electionDeadline = timers.initialElectionTimeout(nodeId);
        network.register(nodeId, this);
    }

    public String getNodeId() { return nodeId; }
    public long getCurrentTerm() { return currentTerm; }
    public NodeRole getRole() { return role; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastFenceSequence() { return lastFenceSequence; }
    public long getLastLogIndex() { return raftLog.isEmpty() ? 0 : raftLog.get(raftLog.size() - 1).getIndex(); }
    public long getLastLogTerm() { return raftLog.isEmpty() ? 0 : raftLog.get(raftLog.size() - 1).getTerm(); }
    public long getLeaseUntilMillis() { return leaseUntilMillis; }

    public boolean isLeaderLeaseValid() {
        return role == NodeRole.LEADER && control.nowMillis() < leaseUntilMillis;
    }

    // ============================ 恢复与持久化 ============================

    /** 从持久化检查点恢复：重放已提交日志重建状态机，不会重复产生写入效果。 */
    public void restore() {
        Checkpoint cp = store.load(nodeId);
        if (cp == null) {
            log.info("[恢复] 节点={} 无历史检查点，以空状态启动", nodeId);
            return;
        }
        loadFromCheckpoint(cp);
        log.info("[恢复] 节点={} 从检查点恢复: 任期={}, 日志末位={}, 提交位点={}, 已应用={}, 幂等记录={}",
                nodeId, currentTerm, getLastLogIndex(), commitIndex, lastApplied, idempotentResults.size());
    }

    /**
     * 从外部给定的检查点恢复（例如误用旧备份重启）。
     * 任期、日志位点或提交位点相对已持久化状态回退时，显式拒绝。
     */
    public void restoreFrom(Checkpoint candidate) {
        long candidateLastLog = candidate.log == null || candidate.log.isEmpty() ? 0
                : candidate.log.get(candidate.log.size() - 1).getIndex();
        store.ensureNoRollback(nodeId, candidate.currentTerm, candidateLastLog, candidate.commitIndex);
        loadFromCheckpoint(candidate);
        log.info("[恢复] 节点={} 接受外部检查点: 任期={}, 提交位点={}", nodeId, candidate.currentTerm, candidate.commitIndex);
    }

    private void loadFromCheckpoint(Checkpoint cp) {
        currentTerm = cp.currentTerm;
        votedFor = cp.votedFor;
        raftLog.clear();
        raftLog.addAll(cp.log);
        commitIndex = cp.commitIndex;
        lastFenceSequence = cp.lastFenceSequence;
        leaseUntilMillis = 0;
        role = NodeRole.FOLLOWER;
        votesGranted.clear();
        // 从空状态重放已提交日志：幂等记录随重放重建，已提交操作只生效一次（初始化语义，而非重复应用）
        stateMachine.locks().clear();
        stateMachine.kv().clear();
        idempotentResults.clear();
        long replayed = 0;
        for (LogEntry e : raftLog) {
            if (e.getIndex() > commitIndex) {
                break;
            }
            Outcome o = stateMachine.apply(e, e.getProposedAtMillis());
            idempotentResults.put(e.getIdempotencyKey(), o);
            replayed++;
        }
        lastApplied = Math.min(commitIndex, getLastLogIndex());
        electionDeadline = control.nowMillis() + timers.initialElectionTimeout(nodeId);
        log.info("[恢复] 节点={} 重放已提交条目 {} 条, KV={}, 锁表={}",
                nodeId, replayed, stateMachine.kv().size(), stateMachine.locks().size());
    }

    private void persist() {
        Checkpoint cp = new Checkpoint();
        cp.nodeId = nodeId;
        cp.currentTerm = currentTerm;
        cp.votedFor = votedFor;
        cp.log = new ArrayList<>(raftLog);
        cp.commitIndex = commitIndex;
        cp.lastApplied = lastApplied;
        cp.lastFenceSequence = lastFenceSequence;
        cp.kvSnapshot = new LinkedHashMap<>(stateMachine.kv());
        cp.idempotent = new LinkedHashMap<>();
        for (Map.Entry<String, Outcome> en : idempotentResults.entrySet()) {
            Outcome o = en.getValue();
            cp.idempotent.put(en.getKey(), new Checkpoint.IdempotentRecord(
                    o.accepted(), o.reason() == null ? null : o.reason().name(),
                    o.fenceToken(), o.detail(), o.kvValue(), commitIndex));
        }
        store.save(cp);
    }

    // ============================ 时间推进 ============================

    public void tick(long nowMillis) {
        switch (role) {
            case FOLLOWER, CANDIDATE -> {
                if (nowMillis < electionDeadline) {
                    return;
                }
                if (nowMillis < lastLeaderLeaseHintUntil) {
                    // 上一任领导者租约可能仍有效，必须等其过期后才能发起选举
                    electionDeadline = nowMillis + 1;
                    return;
                }
                startElection(nowMillis);
            }
            case LEADER -> {
                if (nowMillis >= leaseUntilMillis) {
                    // 保留 LEADER 角色直到见到更高任期：这样 propose() 能区分
                    // “租约过期”(LEADER_LEASE_EXPIRED) 与“根本不是领导者”(NOT_LEADER)。
                    // isLeaderLeaseValid() 对该节点返回 false，它也不会被承认为有效领导者。
                    log.warn("[选举] 节点={} 领导者租约已于 {} 过期(当前={})，失去多数派支持，停止接受写",
                            nodeId, leaseUntilMillis, nowMillis);
                    return;
                }
                if (nowMillis - lastHeartbeatSent >= timers.heartbeatIntervalMillis()) {
                    broadcast(nowMillis);
                    lastHeartbeatSent = nowMillis;
                }
            }
        }
    }

    // ============================ 选举 ============================

    private void startElection(long now) {
        electionAttempt++;
        role = NodeRole.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        votesGranted.clear();
        votesGranted.add(nodeId);
        electionDeadline = now + timers.nextElectionTimeout(nodeId, electionAttempt);
        persist();
        log.info("[选举] 节点={} 发起选举: 任期={}, 判定=选举超时且已知旧领导者租约提示({})已过期",
                nodeId, currentTerm, lastLeaderLeaseHintUntil);
        for (String peer : peers()) {
            network.send(new RaftMessage.RequestVote(currentTerm, nodeId, peer,
                    getLastLogIndex(), getLastLogTerm(), lastLeaderLeaseHintUntil));
        }
        // 单节点集群直接胜选
        if (votesGranted.size() >= quorum) {
            becomeLeader(now);
        }
    }

    private void becomeLeader(long now) {
        role = NodeRole.LEADER;
        for (String peer : peers()) {
            nextIndex.put(peer, getLastLogIndex() + 1);
            matchIndex.put(peer, 0L);
            peerLastAckAt.put(peer, 0L);
        }
        leaseUntilMillis = now + timers.leaderLeaseMillis();
        lastHeartbeatSent = now;
        log.info("[选举] 节点={} 成为领导者: 任期={}, 获得票数={}/{}, 领导者租约至={}",
                nodeId, currentTerm, votesGranted.size(), allNodeIds.size(), leaseUntilMillis);
        // 当选后立即广播租约提示，消除“当选但他人尚不知情”的窗口
        broadcast(now);
    }

    private void handleRequestVote(RaftMessage.RequestVote req, long now) {
        boolean grant = false;
        String basis;
        if (req.term() < currentTerm) {
            basis = "请求任期 " + req.term() + " < 当前任期 " + currentTerm;
        } else {
            if (req.term() > currentTerm) {
                updateTerm(req.term(), null, now);
            }
            if (now < lastLeaderLeaseHintUntil) {
                basis = "已知领导者租约至 " + lastLeaderLeaseHintUntil + " 尚未过期(当前=" + now + ")";
            } else if (role == NodeRole.LEADER) {
                basis = "本节点仍是任期 " + currentTerm + " 的领导者";
            } else if (votedFor != null && !votedFor.equals(req.from())) {
                basis = "本任期已投票给 " + votedFor;
            } else if (req.lastLogTerm() < getLastLogTerm()
                    || (req.lastLogTerm() == getLastLogTerm() && req.lastLogIndex() < getLastLogIndex())) {
                basis = "候选者日志落后(候选=(" + req.lastLogTerm() + "," + req.lastLogIndex()
                        + "), 本地=(" + getLastLogTerm() + "," + getLastLogIndex() + "))";
            } else {
                grant = true;
                votedFor = req.from();
                persist();
                electionDeadline = now + timers.nextElectionTimeout(nodeId, electionAttempt);
                basis = "日志足够新且旧租约已过期";
            }
        }
        log.info("[选举] 节点={} 对 {} 的投票请求(任期={}) => {}: 判定依据={}, 当前任期={}",
                nodeId, req.from(), req.term(), grant ? "同意" : "拒绝", basis, currentTerm);
        network.send(new RaftMessage.VoteResult(currentTerm, nodeId, req.from(), grant,
                lastLeaderLeaseHintUntil));
    }

    private void handleVoteResult(RaftMessage.VoteResult res, long now) {
        if (res.term() > currentTerm) {
            updateTerm(res.term(), null, now);
            return;
        }
        if (!res.granted() || role != NodeRole.CANDIDATE || res.term() != currentTerm) {
            if (!res.granted()) {
                log.debug("[选举] 节点={} 未获得 {} 投票(任期={}), 对端租约提示至 {}",
                        nodeId, res.from(), res.term(), res.lastActiveLeaseUntilMillis());
            }
            return;
        }
        lastLeaderLeaseHintUntil = Math.max(lastLeaderLeaseHintUntil, res.lastActiveLeaseUntilMillis());
        votesGranted.add(res.from());
        log.info("[选举] 节点={} 获得 {} 选票(任期={}), 票数={}/{}",
                nodeId, res.from(), currentTerm, votesGranted.size(), quorum);
        if (votesGranted.size() >= quorum) {
            becomeLeader(now);
        }
    }

    private void updateTerm(long newTerm, String vote, long now) {
        if (newTerm < currentTerm) {
            // 旧任期消息绝不能回滚任期
            return;
        }
        if (role == NodeRole.LEADER && newTerm > currentTerm) {
            log.warn("[选举] 节点={} 见到更高任期 {} > {}，放弃领导身份，旧任期不再接受写",
                    nodeId, newTerm, currentTerm);
        }
        currentTerm = newTerm;
        votedFor = vote;
        role = NodeRole.FOLLOWER;
        votesGranted.clear();
        electionDeadline = now + timers.nextElectionTimeout(nodeId, electionAttempt);
        persist();
    }

    // ============================ 日志复制 ============================

    private void broadcast(long now) {
        for (String peer : peers()) {
            replicateTo(peer);
        }
    }

    private void replicateTo(String peer) {
        long prevIndex = nextIndex.getOrDefault(peer, getLastLogIndex() + 1) - 1;
        long prevTerm = termAt(prevIndex);
        List<LogEntry> entries = new ArrayList<>();
        for (LogEntry e : raftLog) {
            if (e.getIndex() > prevIndex) {
                entries.add(e);
            }
        }
        network.send(new RaftMessage.AppendEntries(currentTerm, nodeId, peer,
                prevIndex, prevTerm, entries, commitIndex, leaseUntilMillis));
    }

    private long termAt(long index) {
        if (index <= 0 || index > getLastLogIndex()) {
            return 0;
        }
        return raftLog.get((int) index - 1).getTerm();
    }

    private void handleAppendEntries(RaftMessage.AppendEntries req, long now) {
        if (req.term() < currentTerm) {
            network.send(new RaftMessage.AppendResult(currentTerm, nodeId, req.from(), false,
                    0, lastLeaderLeaseHintUntil));
            return;
        }
        if (req.term() > currentTerm) {
            updateTerm(req.term(), null, now);
        } else if (role == NodeRole.CANDIDATE) {
            role = NodeRole.FOLLOWER;
            votesGranted.clear();
        } else if (role == NodeRole.LEADER) {
            // 同任期出现另一个领导者：选举安全被破坏的唯一可能是实现缺陷，直接拒绝并告警
            log.error("[选举] 节点={} 在同一任期={} 收到另一领导者 {} 的 AppendEntries，拒绝（双主防护）",
                    nodeId, currentTerm, req.from());
            network.send(new RaftMessage.AppendResult(currentTerm, nodeId, req.from(), false,
                    matchIndex.getOrDefault(req.from(), 0L), lastLeaderLeaseHintUntil));
            return;
        }

        // 记录领导者租约提示：到期前不会投票给任何人
        if (req.leaderLeaseHintUntilMillis() > lastLeaderLeaseHintUntil) {
            lastLeaderLeaseHintUntil = req.leaderLeaseHintUntilMillis();
        }
        electionDeadline = now + timers.nextElectionTimeout(nodeId, electionAttempt);

        boolean consistent = req.prevLogIndex() == 0 || termAt(req.prevLogIndex()) == req.prevLogTerm();
        if (!consistent) {
            log.debug("[复制] 节点={} 拒绝 {} 的 AppendEntries: 前缀不一致 prev=(idx={},term={})",
                    nodeId, req.from(), req.prevLogIndex(), req.prevLogTerm());
            network.send(new RaftMessage.AppendResult(currentTerm, nodeId, req.from(), false,
                    0, lastLeaderLeaseHintUntil));
            return;
        }

        for (LogEntry incoming : req.entries()) {
            int pos = (int) incoming.getIndex() - 1;
            if (pos < raftLog.size()) {
                LogEntry existing = raftLog.get(pos);
                if (existing.getTerm() != incoming.getTerm()) {
                    raftLog.subList(pos, raftLog.size()).clear();
                    raftLog.add(incoming);
                }
            } else {
                raftLog.add(incoming);
            }
        }
        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), getLastLogIndex());
            applyCommitted(now);
        }
        network.send(new RaftMessage.AppendResult(currentTerm, nodeId, req.from(), true,
                getLastLogIndex(), lastLeaderLeaseHintUntil));
    }

    private void handleAppendResult(RaftMessage.AppendResult res, long now) {
        if (res.term() > currentTerm) {
            updateTerm(res.term(), null, now);
            return;
        }
        if (role != NodeRole.LEADER || res.term() != currentTerm) {
            return;
        }
        String peer = res.from();
        if (!res.success()) {
            long back = Math.max(1, nextIndex.getOrDefault(peer, getLastLogIndex() + 1) - 1);
            nextIndex.put(peer, back);
            replicateTo(peer);
            return;
        }
        matchIndex.put(peer, res.matchIndex());
        nextIndex.put(peer, res.matchIndex() + 1);
        peerLastAckAt.put(peer, now);
        if (res.lastActiveLeaseUntilMillis() > lastLeaderLeaseHintUntil) {
            lastLeaderLeaseHintUntil = res.lastActiveLeaseUntilMillis();
        }

        // 推进提交位点（只提交当前任期条目，间接地也提交此前任期条目）
        for (long n = getLastLogIndex(); n > commitIndex; n--) {
            if (termAt(n) != currentTerm) {
                continue;
            }
            int copies = 1; // 领导者自己
            for (String peer2 : peers()) {
                if (matchIndex.getOrDefault(peer2, 0L) >= n) {
                    copies++;
                }
            }
            if (copies >= quorum) {
                commitIndex = n;
                applyCommitted(now);
                break;
            }
        }
        // 若对端仍落后则继续追赶；即时网络下一个调用栈内收敛
        if (nextIndex.getOrDefault(peer, getLastLogIndex() + 1) <= getLastLogIndex()) {
            replicateTo(peer);
        }
        // 多数派仍可达 => 延长领导者租约。
        // 新的租约到期提示只通过“带日志的复制消息”捎带；只有空心跳在途时不重复广播，
        // 避免空心跳 ACK 相互触发形成无限递归。提示的时效性由每 50ms 的周期心跳保证。
        if (quorumActive(now)) {
            leaseUntilMillis = now + timers.leaderLeaseMillis();
            for (String p : peers()) {
                if (matchIndex.getOrDefault(p, 0L) < getLastLogIndex()
                        && network.isReachable(nodeId, p)) {
                    replicateTo(p);
                }
            }
        }
    }

    private boolean quorumActive(long now) {
        int active = 1;
        for (String peer : peers()) {
            Long ack = peerLastAckAt.get(peer);
            if (ack != null && now - ack < timers.leaderLeaseMillis()) {
                active++;
            }
        }
        return active >= quorum;
    }

    private void applyCommitted(long now) {
        while (lastApplied < commitIndex) {
            long idx = lastApplied + 1;
            LogEntry entry = raftLog.get((int) idx - 1);
            Outcome outcome = idempotentResults.get(entry.getIdempotencyKey());
            if (outcome == null) {
                outcome = stateMachine.apply(entry, now);
                idempotentResults.put(entry.getIdempotencyKey(), outcome);
                log.info("[应用] 节点={} 提交 index={}, 任期={}, 幂等键={}, 令牌={}, 结论={}, 判定依据={}",
                        nodeId, idx, entry.getTerm(), entry.getIdempotencyKey(), entry.getFenceToken(),
                        outcome.accepted() ? "接受" : "拒绝:" + outcome.reason(), outcome.detail());
            } else {
                log.info("[应用] 节点={} 跳过重复幂等键 index={}, 幂等键={}, 复用既有结论={}",
                        nodeId, idx, entry.getIdempotencyKey(),
                        outcome.accepted() ? "接受" : "拒绝:" + outcome.reason());
            }
            lastApplied = idx;
        }
        persist();
    }

    // ============================ 客户端写入 ============================

    public Outcome propose(ClientRequest request) {
        long now = control.nowMillis();
        if (role != NodeRole.LEADER) {
            throw new CoordinationException(RejectReason.NOT_LEADER,
                    "节点=" + nodeId + " 不是领导者(角色=" + role + ", 任期=" + currentTerm
                            + "), 幂等键=" + request.idempotencyKey());
        }
        if (now >= leaseUntilMillis) {
            throw new CoordinationException(RejectReason.LEADER_LEASE_EXPIRED,
                    "领导者租约已过期，拒绝写: 节点=" + nodeId + ", 任期=" + currentTerm
                            + ", 租约至=" + leaseUntilMillis + ", 当前=" + now
                            + ", 幂等键=" + request.idempotencyKey()
                            + ", 判定=失去多数派支持，不能再以领导者身份接受写");
        }

        // 幂等快路径：已确定结论的重试/重复提交直接复用，不产生第二条日志
        Outcome existing = idempotentResults.get(request.idempotencyKey());
        if (existing != null) {
            log.info("[幂等] 节点={} 命中已提交结论: 幂等键={}, 任期={}, 令牌={}, 结论={}",
                    nodeId, request.idempotencyKey(), currentTerm, request.fenceToken(),
                    existing.accepted() ? "接受" : "拒绝:" + existing.reason());
            return existing;
        }
        // 在途去重：同一幂等键已有未提交日志（并发提交），不再追加，提交后只应用一次
        for (LogEntry e : raftLog) {
            if (request.idempotencyKey().equals(e.getIdempotencyKey()) && e.getIndex() > lastApplied) {
                log.info("[幂等] 节点={} 抑制在途重复提交: 幂等键={}, 既有index={}, 任期={}",
                        nodeId, request.idempotencyKey(), e.getIndex(), currentTerm);
                return Outcome.ok(e.getFenceToken(), "在途重复提交已抑制，等待同一结论");
            }
        }

        FenceToken token = request.fenceToken();
        if (request.commandType() == CommandType.LOCK_ACQUIRE) {
            lastFenceSequence++;
            token = new FenceToken(currentTerm, lastFenceSequence);
        }
        LogEntry entry = LogEntry.builder()
                .term(currentTerm)
                .index(getLastLogIndex() + 1)
                .commandType(request.commandType())
                .lockKey(request.lockKey())
                .holderId(request.holderId())
                .fenceToken(token)
                .kvKey(request.kvKey())
                .kvValue(request.kvValue())
                .idempotencyKey(request.idempotencyKey())
                .proposedAtMillis(now)
                .lockLeaseMillis(request.lockLeaseMillis())
                .build();
        raftLog.add(entry);
        matchIndex.put(nodeId, entry.getIndex());
        persist();
        log.info("[提案] 节点={} 追加日志 index={}, 任期={}, 命令={}, 幂等键={}, 新令牌={}",
                nodeId, entry.getIndex(), currentTerm, request.commandType(),
                request.idempotencyKey(), token);

        // 即时网络下同步复制，多数派应答后立即提交应用
        for (String peer : peers()) {
            replicateTo(peer);
        }
        Outcome applied = idempotentResults.get(request.idempotencyKey());
        return applied != null ? applied : Outcome.ok(token, "提案已追加，等待多数派提交");
    }

    @Override
    public void receive(RaftMessage message) {
        long now = control.nowMillis();
        switch (message) {
            case RaftMessage.RequestVote rv -> handleRequestVote(rv, now);
            case RaftMessage.VoteResult vr -> handleVoteResult(vr, now);
            case RaftMessage.AppendEntries ae -> handleAppendEntries(ae, now);
            case RaftMessage.AppendResult ar -> handleAppendResult(ar, now);
        }
    }

    public String readKv(String key) {
        long now = control.nowMillis();
        if (role != NodeRole.LEADER) {
            throw new CoordinationException(RejectReason.NOT_LEADER,
                    "读请求被拒: 节点=" + nodeId + " 不是领导者(角色=" + role + ")");
        }
        if (now >= leaseUntilMillis) {
            throw new CoordinationException(RejectReason.LEADER_LEASE_EXPIRED,
                    "读请求被拒: 领导者租约已过期, 节点=" + nodeId + ", 任期=" + currentTerm);
        }
        return stateMachine.readKv(key);
    }

    public Outcome getIdempotentResult(String idemKey) {
        return idempotentResults.get(idemKey);
    }

    private List<String> peers() {
        List<String> result = new ArrayList<>();
        for (String id : allNodeIds) {
            if (!id.equals(nodeId)) {
                result.add(id);
            }
        }
        return result;
    }
}
