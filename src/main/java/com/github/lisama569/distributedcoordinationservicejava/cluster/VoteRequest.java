package com.github.lisama569.distributedcoordinationservicejava.cluster;

/**
 * 拉票请求（RequestVote RPC）。
 */
public record VoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
}
