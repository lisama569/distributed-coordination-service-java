package com.github.lisama569.distributedcoordinationservicejava.recovery;

/**
 * 崩溃恢复检查点（持久化快照）。
 *
 * @param currentTerm 最后已知任期（单调递增，重启后不得回退）
 * @param votedFor    当前任期投票对象（可空）
 * @param lastIndex   已持久化日志的最后下标
 * @param appliedIndex 已应用到状态机的最后下标
 */
public record Checkpoint(long currentTerm, String votedFor, long lastIndex, long appliedIndex) {

    public static Checkpoint empty() {
        return new Checkpoint(0L, null, 0L, 0L);
    }
}
