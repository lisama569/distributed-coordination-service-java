package com.github.lisama569.distributedcoordinationservicejava.core;

/**
 * 可控的逻辑时钟：测试通过显式推进时间来模拟租约过期与选举超时，
 * 避免依赖真实 wall-clock 带来的不稳定性。整个进程内集群共享同一实例。
 */
public class SimClock {

    private long nowMillis;

    public SimClock() {
        this(0L);
    }

    public SimClock(long startMillis) {
        this.nowMillis = startMillis;
    }

    public long nowMillis() {
        return nowMillis;
    }

    /** 仅允许单调向前推进，不允许把时钟拨回（避免“制造”出未过期的旧租约）。 */
    public void advance(long deltaMillis) {
        if (deltaMillis < 0L) {
            throw new IllegalArgumentException("逻辑时钟不允许回退: delta=" + deltaMillis);
        }
        this.nowMillis += deltaMillis;
    }
}
