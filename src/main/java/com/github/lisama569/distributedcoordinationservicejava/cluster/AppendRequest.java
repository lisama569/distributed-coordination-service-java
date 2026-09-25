package com.github.lisama569.distributedcoordinationservicejava.cluster;

import com.github.lisama569.distributedcoordinationservicejava.log.LogEntry;

/**
 * 日志复制 / 心跳请求（AppendEntries RPC）。entries 为空即心跳。
 */
public record AppendRequest(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                            long leaderCommit, java.util.List<LogEntry> entries) {

    public static AppendRequest heartbeat(long term, String leaderId, long leaderCommit) {
        return new AppendRequest(term, leaderId, 0L, 0L, leaderCommit, java.util.List.of());
    }
}
