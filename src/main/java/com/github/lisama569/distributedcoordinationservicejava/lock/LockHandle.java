package com.github.lisama569.distributedcoordinationservicejava.lock;

/**
 * 一次成功持锁的句柄。
 *
 * @param token           栅栏令牌（随任期单调递增）
 * @param acquiredAtMillis 获取时刻（逻辑时钟）
 * @param expireAtMillis   租约到期时刻
 */
public record LockHandle(FenceToken token, long acquiredAtMillis, long expireAtMillis) {
}
