package com.github.lisama569.distributedcoordinationservicejava.snapshot;

import java.util.Map;

/**
 * 历史压缩产生的紧凑状态存档（快照）。
 *
 * <p>只允许覆盖<b>已经提交并应用</b>的位点，因此存档与"从头完整重放 1..lastIncludedIndex
 * 全部已提交日志"严格等价：存档既包含状态机数据，也包含该位点之前全部已生效的
 * 幂等写入索引，之后节点恢复或新建都先安装本存档，再补 lastIncludedIndex 之后的增量日志。
 *
 * @param lastIncludedIndex 存档覆盖的最后日志位点（该位点已提交应用）
 * @param lastIncludedTerm  该位点对应日志的任期
 * @param currentTerm       制作存档时节点已知的最大任期（单调递增，安装后不得回退）
 * @param data              状态机在该位点的完整 K/V 数据
 * @param idempotent        该位点（含）之前全部已生效幂等写: idempotencyKey -> (term, index, value)
 */
public record Snapshot(long lastIncludedIndex, long lastIncludedTerm, long currentTerm,
                       Map<String, String> data, Map<String, IdempotentRecord> idempotent) {

    /** 空存档：代表"没有任何已压缩历史"，节点恢复仍走全量日志重放。 */
    public static Snapshot empty() {
        return new Snapshot(0L, 0L, 0L, Map.of(), Map.of());
    }

    /** 存档内一条已生效幂等写的摘要（用于恢复幂等索引，保证压缩后重试仍只产生一次写入效果）。 */
    public record IdempotentRecord(long term, long index, String value) {
    }
}
