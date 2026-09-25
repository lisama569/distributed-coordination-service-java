package com.github.lisama569.distributedcoordinationservicejava.cluster;

/**
 * 拉票响应。
 *
 * @param term    接收方当前任期（候选人据此回退）
 * @param granted 是否同意
 */
public record VoteResponse(long term, boolean granted) {
}
