package com.github.lisama569.distributedcoordinationservicejava.recovery;

import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import java.util.List;

/**
 * 节点本地“磁盘”抽象：进程内用 {@link InMemoryDisk} 模拟，
 * 节点崩溃（内存状态丢失）后检查点与日志仍保留，重启时据此恢复。
 */
public interface DurableStorage {

    Checkpoint loadCheckpoint();

    /** 写入检查点；任期/位点回退必须抛出 {@code CHECKPOINT_ROLLBACK}。 */
    void saveCheckpoint(Checkpoint checkpoint);

    /** 追加日志；index 必须严格连续、term 不得小于既有条目。 */
    void appendEntry(LogEntry entry);

    List<LogEntry> loadEntries(long fromIndex);

    LogEntry loadEntry(long index);

    long lastIndex();

    long lastTerm();

    /** 模拟磁盘损坏/清空（一般测试不使用）。 */
    void reset();
}
