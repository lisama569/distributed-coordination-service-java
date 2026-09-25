package com.github.lisama569.distributedcoordinationservicejava.recovery;

import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;

import java.util.ArrayList;
import java.util.List;

/**
 * 进程内持久化模拟：用堆内对象扮演“磁盘”。
 * 节点崩溃时只丢弃 {@code RaftNode} 内存对象，本对象仍然存活，
 * 重启节点用同一实例即可恢复任期、投票、日志与检查点。
 *
 * <p>关键不变量：任期只能升、日志下标只能连续追加、检查点位点不能回退。
 * 任何回退尝试都以 {@link CoordinationErrorCode#CHECKPOINT_ROLLBACK} 显式拒绝。
 */
public class InMemoryDisk implements DurableStorage {

    private Checkpoint checkpoint = Checkpoint.empty();
    private final List<LogEntry> entries = new ArrayList<>();

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
        if (!entries.isEmpty() && entry.term() < lastTerm()) {
            throw new CoordinationException(CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "拒绝日志任期回退: prevTerm=" + lastTerm()
                            + ", entryTerm=" + entry.term());
        }
        entries.add(entry);
    }

    @Override
    public synchronized List<LogEntry> loadEntries(long fromIndex) {
        if (fromIndex <= 0L || fromIndex > entries.size()) {
            return List.of();
        }
        return List.copyOf(entries.subList((int) fromIndex - 1, entries.size()));
    }

    @Override
    public synchronized LogEntry loadEntry(long index) {
        if (index <= 0L || index > entries.size()) {
            return null;
        }
        return entries.get((int) index - 1);
    }

    @Override
    public synchronized long lastIndex() {
        return entries.size();
    }

    @Override
    public synchronized long lastTerm() {
        return entries.isEmpty() ? 0L : entries.get(entries.size() - 1).term();
    }

    @Override
    public synchronized void reset() {
        entries.clear();
        checkpoint = Checkpoint.empty();
    }
}
