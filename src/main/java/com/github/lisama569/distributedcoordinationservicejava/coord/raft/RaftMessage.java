package com.github.lisama569.distributedcoordinationservicejava.coord.raft;

import java.util.List;

/** 节点间 RPC 消息（由 {@link com.github.lisama569.distributedcoordinationservicejava.coord.sim.Network} 模拟投递）。 */
public sealed interface RaftMessage
        permits RaftMessage.AppendEntries, RaftMessage.AppendResult,
                RaftMessage.RequestVote, RaftMessage.VoteResult {

    long term();
    String from();
    String to();

    record AppendEntries(long term, String from, String to, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit, long leaderLeaseHintUntilMillis)
            implements RaftMessage {
    }

    record AppendResult(long term, String from, String to, boolean success,
                        long matchIndex, long lastActiveLeaseUntilMillis) implements RaftMessage {
    }

    record RequestVote(long term, String from, String to, long lastLogIndex, long lastLogTerm,
                       long leaseInvalidUntilMillis) implements RaftMessage {
    }

    record VoteResult(long term, String from, String to, boolean granted,
                      long lastActiveLeaseUntilMillis) implements RaftMessage {
    }
}
