package com.github.lisama569.distributedcoordinationservicejava.coord.common;

/**
 * 所有可区分的拒绝原因。客户端与测试依据该原因码归类错误，
 * 错误信息中同时打印任期、栅栏令牌与幂等键等判定上下文。
 */
public enum RejectReason {
    NOT_LEADER,
    LEADER_LEASE_EXPIRED,
    LESS_THAN_QUORUM,
    STALE_TERM,
    LOCK_ALREADY_HELD,
    LOCK_NOT_HELD,
    STALE_FENCE_TOKEN,
    LOCK_LEASE_EXPIRED,
    ROLLBACK_REFUSED,
    NO_LEADER
}
