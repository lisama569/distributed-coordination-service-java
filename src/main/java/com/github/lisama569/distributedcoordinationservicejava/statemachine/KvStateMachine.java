package com.github.lisama569.distributedcoordinationservicejava.statemachine;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 极简 K/V 状态机：日志提交后按顺序 apply。
 *
 * <p>幂等应用：以日志下标去重，崩溃恢复时对已提交范围内同一下标的重复 apply
 * 不会产生第二次写入效果；位点回退（小于已应用位点）会被显式拒绝。
 */
public class KvStateMachine {

    private final Map<String, String> data = new LinkedHashMap<>();
    private long appliedIndex;

    public KvStateMachine() {
    }

    /**
     * 应用一条已提交日志。
     *
     * @param index 日志下标，必须严格等于上一应用位点 + 1（或等于已应用位点——重放）
     * @param key   K/V 的键（本实现以日志载荷 value 的 "key=value" 或直接 key 存储，
     *              协调层传入的 key 即状态机键）
     * @param value 写入值
     * @return 该键的最新值
     */
    public synchronized String apply(long index, String key, String value) {
        if (index <= 0L) {
            throw new IllegalArgumentException("非法日志位点: " + index);
        }
        if (index < appliedIndex) {
            // 位点回退绝不允许继续执行
            throw new com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException(
                    com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode.CHECKPOINT_ROLLBACK,
                    "状态机拒绝位点回退: appliedIndex=" + appliedIndex + ", requestIndex=" + index);
        }
        if (index > appliedIndex + 1L) {
            throw new IllegalStateException("不允许跳位点应用: appliedIndex=" + appliedIndex
                    + ", requestIndex=" + index);
        }
        if (index == appliedIndex) {
            // 崩溃恢复后的重放：位点已应用，直接返回现值，不重复写
            return data.get(key);
        }
        data.put(key, value);
        appliedIndex = index;
        return value;
    }

    public synchronized String read(String key) {
        return data.get(key);
    }

    public synchronized Map<String, String> snapshot() {
        return Map.copyOf(data);
    }

    public synchronized long appliedIndex() {
        return appliedIndex;
    }
}
