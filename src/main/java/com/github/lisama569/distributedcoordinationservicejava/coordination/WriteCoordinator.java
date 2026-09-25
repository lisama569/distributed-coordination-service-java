package com.github.lisama569.distributedcoordinationservicejava.coordination;

import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.core.SimClock;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 幂等执行与崩溃恢复的协调层（每个节点一份，随节点一起崩溃/重启）。
 *
 * <ul>
 *   <li><b>恰好一次写入效果</b>：提交前先查内存去重表，命中则直接回放原结论，
 *       不再走 {@code propose}。重试、并发交叠、重复提交在单线程模拟器中表现为
 *       多次方法调用，结论始终一致，日志只有一条；</li>
 *   <li><b>崩溃恢复</b>：重启后 {@link #restore()} 扫描本节点磁盘上的已持久化日志，
 *       重建去重表。已应用过的日志在 RaftNode 恢复时按位点重放、状态机按位点去重，
 *       因此不会重复应用；</li>
 *   <li><b>回退拒绝</b>：任期/位点回退由底层 storage 与状态机显式拒绝
 *       （{@code CHECKPOINT_ROLLBACK}），本层不会在回退后继续执行。</li>
 * </ul>
 */
public class WriteCoordinator {

    private static final Logger log = LoggerFactory.getLogger(WriteCoordinator.class);

    private final SimClock clock;
    private final DurableStorage storage;

    /** idempotencyKey -> 已提交记录（内存缓存；权威依据是已持久化的日志） */
    private final Map<String, CommittedWrite> committed = new HashMap<>();

    public WriteCoordinator(SimClock clock, DurableStorage storage) {
        this.clock = clock;
        this.storage = storage;
    }

    /**
     * 幂等写入。
     *
     * @param leader         接收写的领导者节点
     * @param idempotencyKey 幂等键（非空）
     * @param value          期望写入的值（重试时允许不同，以首次提交的值为准）
     */
    public WriteResult submit(RaftNode leader, String idempotencyKey, String value) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey 不能为空");
        }
        CommittedWrite existing = committed.get(idempotencyKey);
        if (existing != null) {
            log.info("[idempotent node={}] 幂等键={} 命中已提交记录 index={} term={} (依据: 去重表) -> 直接回放，不重复写入",
                    leader.getNodeId(), idempotencyKey, existing.index(), existing.term());
            return new WriteResult(true, true, existing.term(), existing.index(),
                    idempotencyKey, existing.value());
        }
        LogEntry entry = leader.propose(idempotencyKey, value);
        CommittedWrite done = new CommittedWrite(idempotencyKey, value, entry.term(), entry.index());
        committed.put(idempotencyKey, done);
        log.info("[idempotent node={}] 幂等键={} 首次提交成功 index={} term={} (依据: 多数派提交)",
                leader.getNodeId(), idempotencyKey, entry.index(), entry.term());
        return new WriteResult(true, false, done.term(), done.index(), idempotencyKey, done.value());
    }

    /** 重启后从持久化日志重建去重表：只有真实存在的幂等条目会被恢复。 */
    public void restore() {
        committed.clear();
        for (LogEntry entry : storage.loadEntries(1L)) {
            if (entry.idempotencyKey() != null) {
                // 同一幂等键若在历史日志中出现（理论上被去重表阻止），保留最早一条
                committed.putIfAbsent(entry.idempotencyKey(),
                        new CommittedWrite(entry.idempotencyKey(), entry.value(),
                                entry.term(), entry.index()));
            }
        }
        log.info("[idempotent restore] 从日志恢复 {} 条幂等记录", committed.size());
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
