package com.github.lisama569.distributedcoordinationservicejava.recovery;

import java.util.List;
import java.util.Map;

/**
 * 历史压缩产物：把 [1, lastIncludedIndex] 范围内<b>已经提交并应用</b>的日志
 * 整理成的紧凑状态存档。恢复/追赶时先加载存档，再补 lastIncludedIndex 之后的增量，
 * 最终状态必须与从头完整重放完全一致。
 *
 * <ul>
 *   <li>{@code state}：压缩位点处状态机的完整 K/V 快照；</li>
 *   <li>{@code idempotent}：压缩位点处已生效幂等写的去重记录（幂等键 -> 首次结论），
 *       保证压缩/换主后重试同一幂等键仍只产生一次写入效果；</li>
 *   <li>存档只允许单调前进：位点不能倒退，同位点任期不能变化。</li>
 * </ul>
 */
public record Snapshot(long lastIncludedIndex, long lastIncludedTerm,
                       Map<String, String> state,
                       List<IdempotentRecord> idempotent) {

    /** 幂等去重记录：与压缩前日志中“首次生效的写入”一一对应。 */
    public record IdempotentRecord(String idempotencyKey, String value, long term, long index) {
    }
}
