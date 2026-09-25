package com.github.lisama569.distributedcoordinationservicejava.coord.common;

/**
 * 全局模拟逻辑时钟，所有节点共用，保证测试确定性。
 * 时间只能单调向前推进，不允许回退。
 */
public class SimClock {
    private long nowMillis;

    public long now() {
        return nowMillis;
    }

    public void advance(long deltaMillis) {
        if (deltaMillis < 0) {
            throw new IllegalArgumentException("模拟时钟不允许回退: " + deltaMillis);
        }
        nowMillis += deltaMillis;
    }
}
