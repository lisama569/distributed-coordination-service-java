package com.github.lisama569.distributedcoordinationservicejava.coord.tests;

import com.github.lisama569.distributedcoordinationservicejava.coord.client.CoordClient;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.Cluster;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 锁与栅栏测试：竞争失败、租约过期迟到写、令牌过旧三类错误原因可区分；
 * 栅栏令牌随任期单调递增；续期/释放仅在租约有效期内生效。
 */
class LockFenceTest extends AbstractCoordTest {

    private static final String LOCK = "account-1";

    @Test
    void lockContentionRenewalAndFencedWrites() {
        printSeparator("锁：竞争失败/续期/栅栏写");
        Cluster cluster = newThreeNodeCluster();
        awaitLeaderId(cluster, 1000);

        CoordClient a = new CoordClient(cluster, "holder-A");
        CoordClient b = new CoordClient(cluster, "holder-B");

        FenceToken ta = a.acquireLock(LOCK, "acquire-A", 250);
        System.out.printf("[断言] A 持锁: 令牌=%s, 任期=%d%n", ta, ta.term());

        // 竞争失败：B 在 A 租约有效期内抢锁 -> LOCK_NOT_HELD（竞争失败原因可区分）
        CoordinationException busy = assertThrows(CoordinationException.class,
                () -> b.acquireLock(LOCK, "acquire-B", 250));
        assertEquals(RejectReason.LOCK_NOT_HELD, busy.getReason());
        System.out.printf("[断言] B 抢锁被拒: 原因=%s（竞争失败）%n", busy.getReason());

        // 有效租约内栅栏写成功
        a.fencedWrite(LOCK, "balance", "100", ta, "write-1");
        assertEquals("100", a.read("balance"));

        // 续期成功，写入仍然有效
        FenceToken renewed = a.renewLock(LOCK, ta, "renew-A-1", 250);
        cluster.advance(200);
        a.fencedWrite(LOCK, "balance", "200", renewed, "write-2");
        assertEquals("200", a.read("balance"));
        System.out.printf("[断言] A 续期成功 令牌=%s, 租约内继续写: balance=200%n", renewed);

        // 释放
        a.releaseLock(LOCK, renewed, "release-A");
        System.out.println("[断言] A 释放锁成功，之后 B 可获取");
        FenceToken tb = b.acquireLock(LOCK, "acquire-B-retry", 250);
        assertTrue(tb.compareTo(ta) > 0, "新任期/新获取的令牌必须严格大于旧令牌");
        System.out.printf("[断言] B 获取锁: 新令牌=%s > 旧令牌=%s%n", tb, ta);
    }

    @Test
    void staleTokenAndLeaseExpiredWritesAreDistinguishable() {
        printSeparator("栅栏：旧令牌拒绝 + 租约过期迟到写拒绝");
        Cluster cluster = newThreeNodeCluster();
        awaitLeaderId(cluster, 1000);

        CoordClient a = new CoordClient(cluster, "holder-A");
        CoordClient b = new CoordClient(cluster, "holder-B");

        FenceToken ta = a.acquireLock(LOCK, "acq-A", 200);
        a.fencedWrite(LOCK, "k", "v-by-A", ta, "w-A");

        // A 不续期，租约过期；B 获取新锁得到更新的栅栏令牌
        cluster.advance(260);
        FenceToken tb = b.acquireLock(LOCK, "acq-B", 400);
        assertTrue(tb.compareTo(ta) > 0, "令牌必须随时间单调递增: " + tb + " > " + ta);
        System.out.printf("[断言] A 租约过期后 B 获锁: 新令牌=%s, 旧令牌=%s%n", tb, ta);

        // 1) 旧持有者 A 用旧令牌迟到写 -> STALE_FENCE_TOKEN（令牌过旧，优先判定）
        CoordinationException stale = assertThrows(CoordinationException.class,
                () -> a.fencedWrite(LOCK, "k", "v-late-stale", ta, "w-A-late"));
        assertEquals(RejectReason.STALE_FENCE_TOKEN, stale.getReason());
        System.out.printf("[断言] A 旧令牌迟到写被拒: 原因=%s（令牌过旧，与租约过期可区分）%n", stale.getReason());

        // 2) 当前令牌但租约已过期的“迟到写”：直接向状态机注入一条
        //    proposedAt 落在租约之外的条目，必须以 LOCK_LEASE_EXPIRED 拒绝。
        cluster.advance(420);
        long expiredProposedAt = cluster.now();
        var smEntry = com.github.lisama569.distributedcoordinationservicejava.coord.raft.LogEntry.builder()
                .term(tb.term()).index(9999)
                .commandType(com.github.lisama569.distributedcoordinationservicejava.coord.raft.CommandType.KV_WRITE)
                .lockKey(LOCK).fenceToken(tb).kvKey("k").kvValue("v-late-expired")
                .idempotencyKey("synthetic-late-write")
                .proposedAtMillis(expiredProposedAt)
                .build();
        var sm = new com.github.lisama569.distributedcoordinationservicejava.coord.statemachine.StateMachine();
        // 先建立 B 持锁（租约已过期）的状态
        var lockEntry = com.github.lisama569.distributedcoordinationservicejava.coord.raft.LogEntry.builder()
                .term(tb.term()).index(1)
                .commandType(com.github.lisama569.distributedcoordinationservicejava.coord.raft.CommandType.LOCK_ACQUIRE)
                .lockKey(LOCK).holderId("holder-B").fenceToken(tb)
                .idempotencyKey("synthetic-lock").proposedAtMillis(0).lockLeaseMillis(400)
                .build();
        sm.apply(lockEntry, 0);
        var late = sm.apply(smEntry, cluster.now());
        assertFalse(late.accepted());
        assertEquals(RejectReason.LOCK_LEASE_EXPIRED, late.reason());
        System.out.printf("[断言] 当前令牌但租约过期的迟到写被拒: 原因=%s（与令牌过旧区分）%n", late.reason());
        assertNull(sm.readKv("k"), "迟到写不得生效");

        // 真正通过客户端：B 续期前（已过期）再用当前令牌写 -> LOCK_LEASE_EXPIRED
        CoordinationException expired = assertThrows(CoordinationException.class,
                () -> b.fencedWrite(LOCK, "k", "v-expired", tb, "w-B-expired"));
        assertEquals(RejectReason.LOCK_LEASE_EXPIRED, expired.getReason());
        System.out.printf("[断言] B 租约过期后的写被拒: 原因=%s%n", expired.getReason());

        // 过期后续期也必须被拒绝
        CoordinationException renewExpired = assertThrows(CoordinationException.class,
                () -> b.renewLock(LOCK, tb, "renew-B-late", 400));
        assertEquals(RejectReason.LOCK_LEASE_EXPIRED, renewExpired.getReason());
        System.out.printf("[断言] B 租约过期后续期被拒: 原因=%s%n", renewExpired.getReason());
    }
}
