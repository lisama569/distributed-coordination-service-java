package com.github.lisama569.distributedcoordinationservicejava.core;

/**
 * 协调服务所有失败场景的显式归类。
 * 每一种错误都对应可解释、可区分的判定依据，不允许笼统失败。
 */
public enum CoordinationErrorCode {

    /** 当前节点不是领导者（或已下台），不能接受写操作 */
    NOT_LEADER,

    /** 无法获得多数派：选举失败或写入无法提交 */
    NO_QUORUM,

    /** 领导者租约已过期：心跳未能在 TTL 内得到多数派确认 */
    LEASE_EXPIRED,

    /** 锁竞争失败：目标锁仍被其他持有者在有效租约内持有 */
    LOCK_CONTENDED,

    /** 栅栏令牌过旧：锁已进入更新的任期/代次 */
    STALE_FENCE_TOKEN,

    /** 任期过旧：请求任期落后于节点当前任期 */
    STALE_TERM,

    /** 位点或任期发生回退，检查点/日志被显式拒绝 */
    CHECKPOINT_ROLLBACK,

    /** 其他内部错误 */
    INTERNAL
}
