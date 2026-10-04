package com.github.lisama569.distributedcoordinationservicejava.coordination;

import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.core.SimClock;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Snapshot;
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
 *   <li><b>压缩/换主后仍恰好一次</b>：去重表的权威依据是“存档中的去重记录 +
 *       已提交并应用的日志”。新领导者从存档和追赶增量中重建去重表，
 *       换主后重试同一幂等键依旧只回放首次结论；</li>
 *   <li><b>崩溃恢复</b>：重启后 {@link #restore()} 先装载存档去重记录，
 *       再扫描本节点磁盘上处于“已应用”范围内的日志；未提交的日志尾巴
 *       （例如旧领导者被隔离期间的本地写入尝试本就不会落盘，或冲突尾巴被截断）
 *       绝不计入去重表；</li>
 *   <li><b>回退拒绝</b>：任期/位点回退由底层 storage 与状态机显式拒绝
 *       （{@code CHECKPOINT_ROLLBACK}），本层不会在回退后继续执行。</li>
 * </ul>
 */
public class WriteCoordinator {

    private static final Logger log = LoggerFactory.getLogger(WriteCoordinator.class);

    private final SimClock clock;
    private final DurableStorage storage;

    /** idempotencyKey -> 已提交记录（内存缓存；权威依据是存档 + 已应用日志） */
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
            log.info("[idempotent node={}] 幂等键={} 命中已提交记录 index={} term={} (依据: 去重表/存档) -> 直接回放，不重复写入",
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

    /**
     * 重启后重建去重表：先装载存档中的去重记录，
     * 再扫描“已应用”范围内的日志。未提交/未应用的尾巴一律不算已生效。
     */
    public void restore() {
        committed.clear();
        storage.loadSnapshot().ifPresent(this::loadFromSnapshot);
        long appliedIndex = storage.loadCheckpoint().appliedIndex();
        long fromIndex = storage.loadSnapshot()
                .map(s -> s.lastIncludedIndex() + 1L).orElse(1L);
        int fromLog = 0;
        for (LogEntry entry : storage.loadEntries(fromIndex)) {
            if (entry.index() > appliedIndex) {
                break;
            }
            if (entry.idempotencyKey() != null
                    && committed.putIfAbsent(entry.idempotencyKey(),
                        new CommittedWrite(entry.idempotencyKey(), entry.value(),
                                entry.term(), entry.index())) == null) {
                fromLog++;
            }
        }
        log.info("[idempotent restore] 恢复 {} 条幂等记录 (存档 {} 条 + 已应用日志 {} 条)，已应用位点={}",
                committed.size(),
                storage.loadSnapshot().map(s -> s.idempotent().size()).orElse(0),
                fromLog, appliedIndex);
    }

    /** 装载/安装存档时，把存档中的已生效去重记录并入内存表（首次结论优先）。 */
    public void loadFromSnapshot(Snapshot snapshot) {
        int added = 0;
        for (Snapshot.IdempotentRecord r : snapshot.idempotent()) {
            if (committed.putIfAbsent(r.idempotencyKey(),
                    new CommittedWrite(r.idempotencyKey(), r.value(), r.term(), r.index())) == null) {
                added++;
            }
        }
        log.info("[idempotent snapshot] 从存档装载幂等记录: 存档位点={} term={} 新增 {} 条，去重表共 {} 条",
                snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(), added, committed.size());
    }

    /**
     * 节点实时应用一条新提交日志时登记去重（追赶期间补的增量同样经此入口，
     * 因提交位点严格单调，同一条增量只会应用一次，不会重复计数）。
     */
    public void onEntryApplied(LogEntry entry) {
        if (entry.idempotencyKey() != null) {
            committed.putIfAbsent(entry.idempotencyKey(),
                    new CommittedWrite(entry.idempotencyKey(), entry.value(),
                            entry.term(), entry.index()));
        }
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
