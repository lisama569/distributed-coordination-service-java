package com.github.lisama569.distributedcoordinationservicejava.cluster;

/**
 * 日志复制响应。success=false 且 term 更大时，领导者必须下台。
 *
 * <p>拒绝时附带冲突位点提示，让领导者一轮心跳内直接把 nextIndex 跳到真正分叉处，
 * 追赶轮数与落后多少无关（不会再每轮只回退一格）：
 * <ul>
 *   <li>{@code conflictTerm}：拒绝方在 prevLogIndex 处（或冲突位点）看到的任期；0 表示
 *       该位点已被本地存档覆盖，需要领导者改推存档；</li>
 *   <li>{@code conflictIndex}：拒绝方在该任期内的最前日志位点（任期跳转），或其日志尾
 *       位点（任期为 0/缺日志的纯滞后）；</li>
 *   <li>{@code peerSnapIndex}：拒绝方当前存档位点；非 0 表示所需前序已在其存档覆盖范围内。</li>
 * </ul>
 */
public record AppendResponse(long term, boolean success, long matchIndex,
                             long conflictTerm, long conflictIndex, long peerSnapIndex) {

    /** 成功响应：matched 为对端已与领导者对齐的位点。 */
    public static AppendResponse ok(long term, long matched) {
        return new AppendResponse(term, true, matched, 0L, 0L, 0L);
    }

    /**
     * 冲突拒绝。
     *
     * @param conflictTerm  对端在冲突位点的任期（0 = 该位点无日志或已被存档覆盖）
     * @param conflictIndex 任期冲突时给该任期首条位点；缺日志时给对端日志尾位点
     * @param peerSnapIndex 对端存档位点（提示领导者是否需要改推存档）
     */
    public static AppendResponse reject(long term, long conflictTerm,
                                        long conflictIndex, long peerSnapIndex) {
        return new AppendResponse(term, false, 0L, conflictTerm, conflictIndex, peerSnapIndex);
    }
}
