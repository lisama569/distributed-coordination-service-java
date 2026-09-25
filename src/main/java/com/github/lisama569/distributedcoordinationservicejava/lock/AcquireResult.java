package com.github.lisama569.distributedcoordinationservicejava.lock;

/**
 * 加锁/续期/释放尝试的结果。
 *
 * @param acquired 是否成功；false 时 reason 给出可区分原因
 * @param handle   成功时的锁句柄，失败为 null
 * @param reason   失败原因（{@code LOCK_CONTENDED}/{@code NOT_LEADER}/...）
 */
public record AcquireResult(boolean acquired, LockHandle handle, String reason) {

    public static AcquireResult success(LockHandle handle) {
        return new AcquireResult(true, handle, null);
    }

    public static AcquireResult fail(String reason) {
        return new AcquireResult(false, null, reason);
    }
}
