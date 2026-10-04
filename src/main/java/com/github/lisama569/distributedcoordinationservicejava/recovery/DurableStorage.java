package com.github.lisama569.distributedcoordinationservicejava.recovery;

import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;
import com.github.lisama569.distributedcoordinationservicejava.snapshot.Snapshot;

import java.util.List;

/**
 * 节点本地“磁盘”抽象：进程内用 {@link InMemoryDisk} 模拟，
 * 节点崩溃（内存状态丢失）后检查点、存档与日志仍保留，重启时据此恢复。
 *
 * <p>磁盘上的历史由两部分组成：
 * <ol>
 *   <li><b>存档（snapshot）</b>：压缩后的紧凑状态，覆盖位点 {@code 1..lastIncludedIndex}；</li>
 *   <li><b>增量日志</b>：存档位点之后保留的日志，下标从 {@code lastIncludedIndex+1} 开始。</li>
 * </ol>
 * 一旦某位点被存档覆盖，对应日志前缀就允许被截断删除；节点恢复时先安装存档再补增量。
 */
public interface DurableStorage {

    Checkpoint loadCheckpoint();

    /** 写入检查点；任期/位点回退必须抛出 {@code CHECKPOINT_ROLLBACK}。 */
    void saveCheckpoint(Checkpoint checkpoint);

    /** 追加日志；index 必须严格连续（紧接存档位点/现存日志之后）、term 不得小于既有条目。 */
    void appendEntry(LogEntry entry);

    /**
     * 从 fromIndex 起读取尚存的日志尾部；fromIndex 已被存档覆盖时，
     * 返回存档之后的第一条可用日志起的全部条目。
     */
    List<LogEntry> loadEntries(long fromIndex);

    /** 读取指定位点日志；该位点不存在或已被存档覆盖时返回 null。 */
    LogEntry loadEntry(long index);

    long lastIndex();

    long lastTerm();

    /** 读取本地最新存档；从未压缩过时返回 {@link Snapshot#empty()}。 */
    Snapshot loadSnapshot();

    /** 本地仍保留原始日志的最前位点（= 存档位点 + 1；无存档时为 1）。 */
    long firstRetainedIndex();

    /**
     * 落盘一份新存档（位点必须单调推进、任期不得回退）。
     * 调用方负责保证存档内容只来自已提交应用的位点。
     */
    void saveSnapshot(Snapshot snapshot);

    /**
     * 截断并删除 {@code index <= throughIndex} 的旧日志前缀。
     * 只能截断已被存档覆盖的部分；存档之后的增量日志不受影响。
     */
    void compactPrefixThrough(long throughIndex);

    /**
     * 删除 {@code index >= fromIndex} 的日志后缀（处理 AppendEntries 发现的任期冲突）。
     * 不允许截断到存档覆盖范围之内。
     */
    void truncateFrom(long fromIndex);

    /**
     * 安装对端（领导者）推来的存档：覆盖本地存档、删除被覆盖的旧日志前缀，
     * 并把检查点位点推进到存档位点；存档位点必须不落后于本地既有存档。
     * 返回安装后的检查点供调用方恢复内存状态。
     */
    Checkpoint installSnapshot(Snapshot snapshot);

    /** 模拟磁盘损坏/清空（一般测试不使用）。 */
    void reset();
}
