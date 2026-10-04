package com.github.lisama569.distributedcoordinationservicejava.compaction;

import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Snapshot;
import com.github.lisama569.distributedcoordinationservicejava.statemachine.KvStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 历史压缩器：当节点上<b>仍保留的、已经提交并应用</b>的日志条数达到阈值时，
 * 把截至 {@code appliedIndex} 的较早历史整理成一份 {@link Snapshot} 状态存档。
 *
 * <h3>安全边界</h3>
 * <ul>
 *   <li>只压缩到 {@code appliedIndex}：尚未提交/应用的条目（例如领导者本地已落盘
 *       但未获多数派确认的写）绝不进入存档，也不会被删除；</li>
 *   <li>已承诺生效的写入一条不丢：存档的 K/V 是状态机在该位点的完整状态，
 *       去重记录逐条来自已应用日志；</li>
 *   <li>压缩是幂等的：重复在同一位点压缩得到内容一致、位点不前进的存档；</li>
 *   <li>阈值 {@code <=0} 表示关闭自动压缩。</li>
 * </ul>
 */
public class LogCompactor {

    private static final Logger log = LoggerFactory.getLogger(LogCompactor.class);

    private final DurableStorage storage;
    private final KvStateMachine stateMachine;
    private final long thresholdEntries;

    public LogCompactor(DurableStorage storage, KvStateMachine stateMachine, long thresholdEntries) {
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.thresholdEntries = thresholdEntries;
    }

    public long getThresholdEntries() {
        return thresholdEntries;
    }

    /**
     * 检查是否应压缩：保留窗口内已应用的日志条数达到阈值时，
     * 执行压缩并返回新存档位点；否则返回空。
     *
     * @param appliedIndex 调用方真实已应用到状态机的位点（只可能 <= commitIndex）
     */
    public Optional<Long> maybeCompact(long appliedIndex) {
        if (thresholdEntries <= 0L) {
            return Optional.empty();
        }
        long snapshotIndex = storage.loadSnapshot()
                .map(Snapshot::lastIncludedIndex).orElse(0L);
        // 存档之后仍保留在磁盘上的已应用日志条数
        long retainedApplied = appliedIndex - snapshotIndex;
        if (retainedApplied < thresholdEntries) {
            return Optional.empty();
        }
        return Optional.of(compactTo(appliedIndex));
    }

    /**
     * 在 targetIndex 处构建并落盘存档。targetIndex 必须是已经应用的位点。
     */
    public long compactTo(long targetIndex) {
        long existingK = storage.loadSnapshot()
                .map(Snapshot::lastIncludedIndex).orElse(0L);
        if (targetIndex <= existingK) {
            // 已有覆盖该位点的存档：幂等无操作，绝不回退存档
            return existingK;
        }
        if (targetIndex > stateMachine.appliedIndex()) {
            throw new IllegalStateException(
                    "拒绝压缩未应用位点: targetIndex=" + targetIndex
                            + " appliedIndex=" + stateMachine.appliedIndex());
        }
        LogEntry boundary = storage.loadEntry(targetIndex);
        if (boundary == null) {
            // 边界条目可能恰好已被既有存档覆盖之后的第一条……若不在磁盘上则无法压缩
            throw new IllegalStateException(
                    "压缩边界位点缺少日志条目: targetIndex=" + targetIndex
                            + " firstIndex=" + storage.firstIndex());
        }

        // 1) 去重记录：以旧存档为底（首次生效结论优先），逐条并入新应用段的已提交日志
        Map<String, Snapshot.IdempotentRecord> dedup = new LinkedHashMap<>();
        for (Snapshot.IdempotentRecord r : storage.loadSnapshot()
                .map(Snapshot::idempotent).orElse(List.of())) {
            dedup.put(r.idempotencyKey(), r);
        }
        for (LogEntry e : storage.loadEntries(existingK + 1L)) {
            if (e.index() > targetIndex) {
                break;
            }
            if (e.idempotencyKey() != null) {
                dedup.putIfAbsent(e.idempotencyKey(),
                        new Snapshot.IdempotentRecord(e.idempotencyKey(), e.value(),
                                e.term(), e.index()));
            }
        }

        // 2) K/V 状态直接取自状态机：它就是 [1,targetIndex] 全部应用后的权威结果
        Map<String, String> state = stateMachine.snapshot();

        Snapshot snapshot = new Snapshot(targetIndex, boundary.term(),
                Map.copyOf(state), List.copyOf(new ArrayList<>(dedup.values())));
        storage.installSnapshot(snapshot);
        log.info("[compaction] 历史压缩完成: 存档位点 {} (term={})，状态 {} 键，幂等记录 {} 条，保留增量 {} 条 (依据: 仅整理已提交并应用的历史，阈值={})",
                targetIndex, boundary.term(), state.size(), dedup.size(),
                storage.lastIndex() - targetIndex, thresholdEntries);
        return targetIndex;
    }
}
