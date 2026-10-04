package com.github.lisama569.distributedcoordinationservicejava.recovery;

import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * 进程内持久化模拟：用堆内对象扮演“磁盘”。
 * 节点崩溃时只丢弃 {@code RaftNode} 内存对象，本对象仍然存活，
 * 重启节点用同一实例即可恢复任期、投票、存档、日志与检查点。
 *
 * <p>关键不变量：任期只能升、日志下标只能连续追加、检查点位点不能回退、
 * 存档位点只能单调前进。任何回退尝试都以
 * {@link CoordinationErrorCode#CHECKPOINT_ROLLBACK} 显式拒绝。</p>
 *
 * <p>压缩后 [1, snapshot.lastIncludedIndex] 的日志条目被物理删除，
 * 但其状态与去重结论完整保存在 {@link Snapshot} 中。</p>
 */
public class InMemoryDisk implements DurableStorage {

    private Checkpoint checkpoint = Checkpoint.empty();
    /** 仍保留的日志：位点 -> 条目（存档覆盖的位点不在此 map 中） */
    private final TreeMap<Long, LogEntry> entries = new TreeMap<>();
    private Snapshot snapshot;

    public InMemoryDisk() {
    }

    @Override
    public synchronized Checkpoint loadCheckpoint() {
        return checkpoint;
    }

    @Override
    public synchronized void saveCheckpoint(Checkpoint candidate) {
        if (candidate.currentTerm() < checkpoint.currentTerm()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝任期回退: storedTerm=" + checkpoint.currentTerm()
                            + ", requestedTerm=" + candidate.currentTerm());
        }
        if (candidate.lastIndex() < checkpoint.lastIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝日志位点回退: storedLastIndex=" + checkpoint.lastIndex()
                            + ", requestedLastIndex=" + candidate.lastIndex());
        }
        if (candidate.appliedIndex() < checkpoint.appliedIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝应用位点回退: storedAppliedIndex=" + checkpoint.appliedIndex()
                            + ", requestedAppliedIndex=" + candidate.appliedIndex());
        }
        if (candidate.lastIndex() > lastIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "检查点位点超过已持久化日志: lastIndex=" + candidate.lastIndex()
                            + ", persistedLastIndex=" + lastIndex());
        }
        if (candidate.appliedIndex() > candidate.lastIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "应用位点超过日志位点: appliedIndex=" + candidate.appliedIndex()
                            + ", lastIndex=" + candidate.lastIndex());
        }
        this.checkpoint = candidate;
    }

    @Override
    public synchronized void appendEntry(LogEntry entry) {
        long expectedIndex = lastIndex() + 1L;
        if (entry.index() != expectedIndex) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝非连续日志追加: expectedIndex=" + expectedIndex
                            + ", actualIndex=" + entry.index());
        }
        if (entry.term() < lastTerm()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝日志任期回退: prevTerm=" + lastTerm()
                            + ", entryTerm=" + entry.term());
        }
        entries.put(entry.index(), entry);
    }

    @Override
    public synchronized List<LogEntry> loadEntries(long fromIndex) {
        if (fromIndex <= 0L || entries.isEmpty() || fromIndex > entries.lastKey()) {
            return List.of();
        }
        return List.copyOf(new ArrayList<>(entries.tailMap(fromIndex, true).values()));
    }

    @Override
    public synchronized LogEntry loadEntry(long index) {
        return entries.get(index);
    }

    @Override
    public synchronized long lastIndex() {
        if (!entries.isEmpty()) {
            return entries.lastKey();
        }
        return snapshot == null ? 0L : snapshot.lastIncludedIndex();
    }

    @Override
    public synchronized long lastTerm() {
        if (!entries.isEmpty()) {
            return entries.get(entries.lastKey()).term();
        }
        return snapshot == null ? 0L : snapshot.lastIncludedTerm();
    }

    @Override
    public synchronized long firstIndex() {
        if (!entries.isEmpty()) {
            return entries.firstKey();
        }
        // 没有保留条目：下一条应追加在 snapshot 位点之后；从未写过日志时为 1
        return (snapshot == null ? 0L : snapshot.lastIncludedIndex()) + 1L;
    }

    @Override
    public synchronized void installSnapshot(Snapshot candidate) {
        if (candidate == null) {
            throw new IllegalArgumentException("snapshot 不能为空");
        }
        long k = candidate.lastIncludedIndex();
        long existingK = snapshot == null ? 0L : snapshot.lastIncludedIndex();
        if (k < existingK) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝存档位点回退: storedSnapshotIndex=" + existingK
                            + ", requestedSnapshotIndex=" + k);
        }
        if (snapshot != null && k == existingK
                && candidate.lastIncludedTerm() != snapshot.lastIncludedTerm()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝同位点不同任期的存档覆盖: index=" + k
                            + " storedTerm=" + snapshot.lastIncludedTerm()
                            + " requestedTerm=" + candidate.lastIncludedTerm());
        }
        // 物理删除严格早于存档边界的条目；边界条目 K 本身保留，
        // 使存档之后的日志读取保持连续（标准 Raft 的 lastIncluded 保留约定）。
        entries.headMap(k, false).clear();
        this.snapshot = candidate;
        // 存档内容本身就是已应用状态：同步推进检查点，任期/投票保留磁盘权威值
        long newTerm = Math.max(checkpoint.currentTerm(), candidate.lastIncludedTerm());
        long newLast = Math.max(checkpoint.lastIndex(), k);
        long newApplied = Math.max(checkpoint.appliedIndex(), k);
        this.checkpoint = new Checkpoint(newTerm, checkpoint.votedFor(), newLast, newApplied);
    }

    @Override
    public synchronized Optional<Snapshot> loadSnapshot() {
        return Optional.ofNullable(snapshot);
    }

    @Override
    public synchronized void truncateSuffixFrom(long fromIndex) {
        long retainedFloor = snapshot == null ? 1L : snapshot.lastIncludedIndex() + 1L;
        if (fromIndex < retainedFloor) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "不允许截断存档已覆盖的范围: fromIndex=" + fromIndex
                            + " retainedFloor=" + retainedFloor);
        }
        if (!entries.isEmpty() && fromIndex <= entries.lastKey()) {
            new ArrayList<>(entries.tailMap(fromIndex, true).keySet())
                    .forEach(entries::remove);
        }
    }

    /**
     * 测试辅助：模拟节点离线期间其本地 [1, gcIndex] 区段被保留策略物理回收。
     * 磁盘没有存档时，这会在“空洞起点 gcIndex+1”之前留下无法恢复的断层；
     * 磁盘已有存档时，回收范围至少覆盖存档（存档本身仍可加载）。
     */
    public synchronized void simulateOfflineRetentionGcUpTo(long gcIndex) {
        entries.headMap(gcIndex, true).clear();
    }

    @Override
    public synchronized void reset() {
        entries.clear();
        snapshot = null;
        checkpoint = Checkpoint.empty();
    }
}
