package com.github.lisama569.distributedcoordinationservicejava.cluster;

/**
 * 日志复制响应。success=false 且 term 更大时，领导者必须下台。
 *
 * <p>拒绝时携带<b>冲突提示</b>（Raft 冲突快速回退优化），让领导者把 nextIndex
 * 一次性跳到正确位置，而不是一轮心跳只回退一格——落后很多的节点因此能在
 * 少数几轮内追平，追赶轮数基本不随落后量增长：
 * <ul>
 *   <li>{@code conflictTerm = -1}：冲突位点已被接收方存档覆盖，普通增量无法服务，
 *       领导者应改走 InstallSnapshot（或从位点 1 全量对齐）；</li>
 *   <li>{@code conflictTerm = 0}：接收方日志太短，{@code conflictIndex} = 接收方
 *       lastIndex + 1，领导者直接把 nextIndex 设为该值即可；</li>
 *   <li>{@code conflictTerm > 0}：prevLog 处任期冲突，{@code conflictTerm} 为冲突位点
 *       的任期、{@code conflictIndex} 为该任期在接收方日志中的首个下标；领导者若自己
 *       持有该任期的条目则跳到其之后，否则跳到 conflictIndex。</li>
 * </ul>
 */
public record AppendResponse(long term, boolean success, long matchIndex,
                             long conflictTerm, long conflictIndex) {

    /** 兼容旧签名：无冲突提示（领导者按一格回退处理）。 */
    public AppendResponse(long term, boolean success, long matchIndex) {
        this(term, success, matchIndex, 0L, 0L);
    }

    /** 成功响应。 */
    public static AppendResponse ok(long term, long matchIndex) {
        return new AppendResponse(term, true, matchIndex, 0L, 0L);
    }

    /** 拒绝：接收方日志太短，nextIndex 应跳到 followerLastIndex + 1。 */
    public static AppendResponse rejectLogTooShort(long term, long followerLastIndex) {
        return new AppendResponse(term, false, 0L, 0L, followerLastIndex + 1L);
    }

    /** 拒绝：prevLog 任期冲突，携带冲突任期与其首个下标。 */
    public static AppendResponse rejectTermConflict(long term, long conflictTerm, long conflictIndex) {
        return new AppendResponse(term, false, 0L, conflictTerm, conflictIndex);
    }

    /** 拒绝：冲突位点已被本地存档覆盖，领导者需改推存档（conflictTerm = -1 哨兵）。 */
    public static AppendResponse rejectNeedsSnapshot(long term, long snapIndex) {
        return new AppendResponse(term, false, 0L, -1L, snapIndex);
    }
}
