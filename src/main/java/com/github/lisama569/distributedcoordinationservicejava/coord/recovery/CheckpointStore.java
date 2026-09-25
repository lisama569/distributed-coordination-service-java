package com.github.lisama569.distributedcoordinationservicejava.coord.recovery;

/** 检查点持久化抽象（测试可用文件实现，亦可替换为内存实现）。 */
public interface CheckpointStore {

    /** 原子写入检查点。 */
    void save(Checkpoint checkpoint);

    /** 读取检查点；不存在时返回 null。 */
    Checkpoint load(String nodeId);

    /**
     * 恢复时的安全门：位点或任期相对已持久化检查点回退必须显式拒绝。
     */
    void ensureNoRollback(String nodeId, long term, long lastLogIndex, long commitIndex);

    void clear(String nodeId);
}
