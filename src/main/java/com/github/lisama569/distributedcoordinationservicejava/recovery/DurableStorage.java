package com.github.lisama569.distributedcoordinationservicejava.recovery;

import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import java.util.List;
import java.util.Optional;

/**
 * 节点本地“磁盘”抽象：进程内用 {@link InMemoryDisk} 模拟，
 * 节点崩溃（内存状态丢失）后检查点、存档与日志仍保留，重启时据此恢复。
 *
 * <h3>历史压缩后的存储形态</h3>
 * 存档覆盖 [1, snapshot.lastIncludedIndex]，该范围的日志条目被物理删除；
 * {@link #loadEntry} 对“已被存档覆盖”的位点返回 null，但可通过
 * {@link #loadSnapshot()} 取回该位点对应的任期与状态。恢复顺序固定为：
 * 先安装存档，再从 {@link #firstIndex()} 开始补增量。
 */
public interface DurableStorage {

    Checkpoint loadCheckpoint();

    /** 写入检查点；任期/位点回退必须抛出 {@code CHECKPOINT_ROLLBACK}。 */
    void saveCheckpoint(Checkpoint checkpoint);

    /** 追加日志；index 必须严格连续（相对当前保留窗口）、term 不得小于既有条目。 */
    void appendEntry(LogEntry entry);

    /** 从 fromIndex 起读取仍保留的日志（fromIndex 早于保留窗口时返回空列表）。 */
    List<LogEntry> loadEntries(long fromIndex);

    /** 读取仍保留的单条日志；位点为 0、越界或已被存档覆盖时返回 null。 */
    LogEntry loadEntry(long index);

    long lastIndex();

    long lastTerm();

    /** 当前保留日志的最早位点；没有任何保留条目时返回 {@code lastIndex()+1}，从未写过日志时为 1。 */
    long firstIndex();

    /**
     * 安装存档：持久化存档并删除其覆盖范围内的日志条目（含 fromIndex 处冲突条目）。
     * 存档只允许单调前进：位点回退抛 {@code CHECKPOINT_ROLLBACK}，
     * 同位点任期不一致同样拒绝。
     */
    void installSnapshot(Snapshot snapshot);

    /** 当前磁盘上的存档（从未压缩时为空）。 */
    Optional<Snapshot> loadSnapshot();

    /**
     * 删除从 fromIndex 起到日志末尾的全部条目（用于追赶时覆盖冲突的未提交尾巴）。
     * 不允许截断存档已覆盖的范围。
     */
    void truncateSuffixFrom(long fromIndex);

    /** 模拟磁盘损坏/清空（一般测试不使用）。 */
    void reset();
}
