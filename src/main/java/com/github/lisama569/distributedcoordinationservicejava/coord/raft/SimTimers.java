package com.github.lisama569.distributedcoordinationservicejava.coord.raft;

import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;

/**
 * 选举与心跳计时参数（基于模拟时钟）。
 *
 * <p>每个节点持有以节点身份为种子的确定性随机源，在 [min,max] 区间内给出选举超时：
 * 不同节点的超时自然错开，落选后下一轮重新抖动，避免活锁；种子固定保证测试可复现。</p>
 *
 * <p>不变式：{@code leaderLeaseMillis > electionTimeoutMaxMillis}，
 * 保证“旧领导者租约先过期，新领导者才可能当选”，杜绝双主。</p>
 */
public record SimTimers(long heartbeatIntervalMillis,
                        long electionTimeoutMinMillis,
                        long electionTimeoutMaxMillis,
                        long leaderLeaseMillis) {

    public SimTimers {
        if (heartbeatIntervalMillis <= 0 || electionTimeoutMinMillis <= 0
                || electionTimeoutMaxMillis < electionTimeoutMinMillis
                || leaderLeaseMillis <= electionTimeoutMaxMillis) {
            throw new IllegalArgumentException("非法计时参数: heartbeat=" + heartbeatIntervalMillis
                    + ", election=[" + electionTimeoutMinMillis + "," + electionTimeoutMaxMillis
                    + "], lease=" + leaderLeaseMillis
                    + "（领导者租约必须严格大于选举超时上限，保证旧租约过期后才可能选出新领导者）");
        }
    }

    public static SimTimers defaults() {
        return new SimTimers(50, 180, 300, 400);
    }

    /** 节点的初始选举超时（从 0 时刻起）。 */
    public long initialElectionTimeout(String nodeId) {
        return randomFor(nodeId, 0).nextLong(electionTimeoutMinMillis, electionTimeoutMaxMillis + 1);
    }

    /** 某次落选后的下一轮选举超时：同节点不同轮次重新抖动。 */
    public long nextElectionTimeout(String nodeId, int attempt) {
        return randomFor(nodeId, attempt).nextLong(electionTimeoutMinMillis, electionTimeoutMaxMillis + 1);
    }

    private RandomGenerator randomFor(String nodeId, int attempt) {
        long seed = ((long) nodeId.hashCode() * 1_000_003L) ^ (attempt * 0x9E3779B97F4A7C15L);
        return RandomGeneratorFactory.<RandomGenerator>of("L64X128MixRandom")
                .create(seed);
    }
}
