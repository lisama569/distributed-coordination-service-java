package com.github.lisama569.distributedcoordinationservicejava.recovery;

import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.snapshot.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 进程内持久化模拟：用堆内对象扮演“磁盘”。
 * 节点崩溃时只丢弃 {@code RaftNode} 内存对象，本对象仍然存活，
 * 重启节点用同一实例即可恢复任期、投票、存档、日志与检查点。
 *
 * <p>磁盘布局：一份 {@link Snapshot}（可为空存档）覆盖 {@code 1..lastIncludedIndex}，
 * 之后的增量日志保存在 TreeMap 中，键为日志下标。历史压缩 = 先落盘更靠后的存档，
 * 再 {@link #compactPrefixThrough} 删除被覆盖的日志前缀。
 *
 * <p>关键不变量：任期只能升、日志下标只能连续追加、存档位点/检查点位点不能回退。
 * 任何回退尝试都以 {@link CoordinationErrorCode#CHECKPOINT_ROLLBACK} 显式拒绝。
 */
public class InMemoryDisk implements DurableStorage {

    private static final Logger log = LoggerFactory.getLogger(InMemoryDisk.class);

    private Checkpoint checkpoint = Checkpoint.empty();
    private Snapshot snapshot = Snapshot.empty();
    /** key = 日志下标；只保留存档位点之后的增量日志。 */
    private final TreeMap<Long, LogEntry> entries = new TreeMap<>();

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
                    "检查点位点超过已持久化历史(存档+日志): lastIndex=" + candidate.lastIndex()
                            + ", persistedLastIndex=" + lastIndex());
        }
        if (candidate.appliedIndex() > lastIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "应用位点超过已持久化历史(存档+日志): appliedIndex=" + candidate.appliedIndex()
                            + ", persistedLastIndex=" + lastIndex());
        }
        this.checkpoint = candidate;
    }

    @Override
    public synchronized void appendEntry(LogEntry entry) {
        // 允许首条日志紧接存档位点之后（空盘重置/安装存档后的节点从全局位点续接）
        long expectedIndex = entries.isEmpty()
                ? snapshot.lastIncludedIndex() + 1L
                : entries.lastKey() + 1L;
        if (entry.index() != expectedIndex) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝非连续日志追加: expectedIndex=" + expectedIndex
                            + ", actualIndex=" + entry.index()
                            + " (snapIndex=" + snapshot.lastIncludedIndex() + ")");
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
        long start = Math.max(fromIndex, firstRetainedIndex());
        if (start > lastIndex()) {
            return List.of();
        }
        return List.copyOf(entries.tailMap(start, true).values());
    }

    @Override
    public synchronized LogEntry loadEntry(long index) {
        return entries.get(index);
    }

    @Override
    public synchronized long lastIndex() {
        return entries.isEmpty() ? snapshot.lastIncludedIndex() : entries.lastKey();
    }

    @Override
    public synchronized long lastTerm() {
        Map.Entry<Long, LogEntry> last = entries.lastEntry();
        return last == null ? snapshot.lastIncludedTerm() : last.getValue().term();
    }

    @Override
    public synchronized Snapshot loadSnapshot() {
        return snapshot;
    }

    @Override
    public synchronized long firstRetainedIndex() {
        return snapshot.lastIncludedIndex() + 1L;
    }

    @Override
    public synchronized void saveSnapshot(Snapshot candidate) {
        if (candidate.lastIncludedIndex() < snapshot.lastIncludedIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝存档位点回退: storedSnapshotIndex=" + snapshot.lastIncludedIndex()
                            + ", requestedIndex=" + candidate.lastIncludedIndex());
        }
        if (candidate.currentTerm() < snapshot.currentTerm()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝存档任期回退: storedSnapshotTerm=" + snapshot.currentTerm()
                            + ", requestedTerm=" + candidate.currentTerm());
        }
        if (candidate.lastIncludedIndex() > checkpoint.appliedIndex()) {
            // 安全红线：尚未应用（可能尚未确认生效）的内容绝不允许进入存档
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝压缩未应用位点: snapshotIndex=" + candidate.lastIncludedIndex()
                            + ", appliedIndex=" + checkpoint.appliedIndex());
        }
        this.snapshot = candidate;
    }

    @Override
    public synchronized void compactPrefixThrough(long throughIndex) {
        if (throughIndex > snapshot.lastIncludedIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝截断未存档的日志前缀: compactIndex=" + throughIndex
                            + ", snapshotIndex=" + snapshot.lastIncludedIndex());
        }
        int before = entries.size();
        entries.headMap(throughIndex, true).clear();
        int removed = before - entries.size();
        if (removed > 0) {
            log.debug("本地磁盘截断已压缩日志前缀 throughIndex={} 删除条数={}", throughIndex, removed);
        }
    }

    @Override
    public synchronized void truncateFrom(long fromIndex) {
        if (fromIndex <= snapshot.lastIncludedIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝截断到存档覆盖范围之内: fromIndex=" + fromIndex
                            + ", snapshotIndex=" + snapshot.lastIncludedIndex());
        }
        entries.tailMap(fromIndex, true).clear();
    }

    @Override
    public synchronized Checkpoint installSnapshot(Snapshot incoming) {
        if (incoming.lastIncludedIndex() < snapshot.lastIncludedIndex()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝安装更旧的存档: localSnapshotIndex=" + snapshot.lastIncludedIndex()
                            + ", incomingIndex=" + incoming.lastIncludedIndex());
        }
        if (incoming.currentTerm() < currentTermFloor()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝安装任期回退的存档: localTerm=" + currentTermFloor()
                            + ", incomingTerm=" + incoming.currentTerm());
        }
        // 删除被存档覆盖的旧日志前缀；之后的增量日志保留，等领导者用 AppendEntries 对齐
        entries.headMap(incoming.lastIncludedIndex(), true).clear();
        this.snapshot = incoming;
        Checkpoint installed = new Checkpoint(incoming.currentTerm(), checkpoint.votedFor(),
                incoming.lastIncludedIndex(), incoming.lastIncludedIndex());
        this.checkpoint = installed;
        return installed;
    }

    @Override
    public synchronized void reset() {
        entries.clear();
        snapshot = Snapshot.empty();
        checkpoint = Checkpoint.empty();
    }

    private long currentTermFloor() {
        return Math.max(checkpoint.currentTerm(), snapshot.currentTerm());
    }
}
