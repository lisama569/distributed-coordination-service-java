package com.github.lisama569.distributedcoordinationservicejava.coordination;

import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.core.SimClock;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import com.github.lisama569.distributedcoordinationservicejava.snapshot.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 幂等执行与崩溃恢复的协调层（每个节点一份，随节点一起崩溃/重启）。
 *
 * <ul>
 *   <li><b>恰好一次写入效果</b>：提交前先查接收节点的<b>已应用幂等索引</b>
 *       （{@link RaftNode#findAppliedIdempotent}，同时覆盖历史存档与增量日志，是权威来源），
 *       命中则直接回放原结论，不再走 {@code propose}；本地 {@code committed} 缓存作为
 *       本节点本任期内的快速路径。重试、换主后重试、压缩后重启重试、追赶补齐后的重试，
 *       结论始终一致，日志只有一条；</li>
 *   <li><b>崩溃恢复</b>：重启后 {@link #restore()} 从本节点磁盘上的存档 + 尚存增量日志
 *       重建去重表。已应用过的日志在 RaftNode 恢复时按位点重放、状态机按位点去重，
 *       存档安装是整体替换，因此追赶/恢复都不会重复应用；</li>
 *   <li><b>回退拒绝</b>：任期/位点/存档回退由底层 storage 与状态机显式拒绝
 *       （{@code CHECKPOINT_ROLLBACK}），本层不会在回退后继续执行。</li>
 * </ul>
 */
public class WriteCoordinator {

    private static final Logger log = LoggerFactory.getLogger(WriteCoordinator.class);

    private final SimClock clock;
    private final DurableStorage storage;

    /** idempotencyKey -> 已提交记录（本地缓存；权威依据是节点已应用索引/磁盘存档+日志） */
    private final Map<String, CommittedWrite> committed = new HashMap<>();

    public WriteCoordinator(SimClock clock, DurableStorage storage) {
        this.clock = clock;
        this.storage = storage;
    }

    /**
     * 幂等写入。
     *
     * @param leader         接收写的节点（当前有有效租约的领导者）
     * @param idempotencyKey 幂等键（非空）
     * @param value          期望写入的值（重试时允许不同，以首次提交的值为准）
     */
    public WriteResult submit(RaftNode leader, String idempotencyKey, String value) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey 不能为空");
        }
        // 权威判定：节点已应用索引（存档 + 增量日志）里是否已有该键。
        // 这一步同时覆盖：同节点重试、换主后落到新领导者、压缩后重启、追赶补齐后。
        Snapshot.IdempotentRecord applied = leader.findAppliedIdempotent(idempotencyKey);
        if (applied != null) {
            log.info("[idempotent node={}] 幂等键={} 命中已生效记录 index={} term={} (依据: 节点已应用索引/存档) -> 直接回放，不重复写入",
                    leader.getNodeId(), idempotencyKey, applied.index(), applied.term());
            committed.putIfAbsent(idempotencyKey, new CommittedWrite(
                    idempotencyKey, applied.value(), applied.term(), applied.index()));
            return new WriteResult(true, true, applied.term(), applied.index(),
                    idempotencyKey, applied.value());
        }
        CommittedWrite existing = committed.get(idempotencyKey);
        if (existing != null) {
            log.info("[idempotent node={}] 幂等键={} 命中本地去重表 index={} term={} (依据: 去重表) -> 直接回放，不重复写入",
                    leader.getNodeId(), idempotencyKey, existing.index(), existing.term());
            return new WriteResult(true, true, existing.term(), existing.index(),
                    idempotencyKey, existing.value());
        }
        LogEntry entry = leader.propose(idempotencyKey, value);
        CommittedWrite done = new CommittedWrite(idempotencyKey, value, entry.term(), entry.index());
        committed.put(idempotencyKey, done);
        log.info("[idempotent node={}] 幂等键={} 首次提交成功 index={} term={} (依据: 多数派提交并应用)",
                leader.getNodeId(), idempotencyKey, entry.index(), entry.term());
        return new WriteResult(true, false, done.term(), done.index(), idempotencyKey, done.value());
    }

    /**
     * 重启后重建去重表：先取历史存档里的全部已生效幂等记录，再叠加存档之后尚存的
     * 增量日志（同一键保留最早一条）。只有真实生效的幂等写会被恢复。
     */
    public void restore() {
        committed.clear();
        Snapshot snapshot = storage.loadSnapshot();
        for (Map.Entry<String, Snapshot.IdempotentRecord> e : snapshot.idempotent().entrySet()) {
            Snapshot.IdempotentRecord r = e.getValue();
            committed.put(e.getKey(), new CommittedWrite(e.getKey(), r.value(), r.term(), r.index()));
        }
        for (LogEntry entry : storage.loadEntries(snapshot.lastIncludedIndex() + 1L)) {
            if (entry.idempotencyKey() != null) {
                // 同一幂等键若在历史中出现（理论上被去重表阻止），保留最早一条
                committed.putIfAbsent(entry.idempotencyKey(),
                        new CommittedWrite(entry.idempotencyKey(), entry.value(),
                                entry.term(), entry.index()));
            }
        }
        log.info("[idempotent restore] 从存档({} 条, snapIndex={})+增量日志恢复 {} 条幂等记录",
                snapshot.idempotent().size(), snapshot.lastIncludedIndex(), committed.size());
    }

    public boolean isCommitted(String idempotencyKey) {
        return committed.containsKey(idempotencyKey);
    }

    public CommittedWrite getCommitted(String idempotencyKey) {
        return committed.get(idempotencyKey);
    }

    /** 已提交的幂等写记录。 */
    public record CommittedWrite(String idempotencyKey, String value, long term, long index) {
    }
}
