package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.lock.AcquireResult;
import com.github.lisama569.distributedcoordinationservicejava.lock.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.lock.LockHandle;
import com.github.lisama569.distributedcoordinationservicejava.lock.LockService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 租约锁场景：
 * <ol>
 *   <li>锁竞争失败 -> LOCK_CONTENDED，可与租约/令牌错误区分；</li>
 *   <li>TTL 内续期成功；TTL 过后旧持有者迟到写 -> LEASE_EXPIRED；</li>
 *   <li>锁被新任期持有者拿走后，旧令牌写 -> STALE_FENCE_TOKEN；</li>
 *   <li>栅栏令牌随任期单调递增，不会出现新锁令牌 ≤ 旧锁令牌。</li>
 * </ol>
 */
class LeaseLockTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseLockTest.class);

    @Test
    void contentionIsDistinguishableFromLeaseAndTokenErrors() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        LockService locks = cluster.lockService();

        AcquireResult first = locks.tryAcquire(leader, "order-1", "clientA");
        assertTrue(first.acquired());
        log.info("[scenario-竞争] clientA 加锁成功 幂等键(order)=order-1 栅栏令牌={} term={}",
                first.handle().token(), leader.getCurrentTerm());

        AcquireResult second = locks.tryAcquire(leader, "order-1", "clientB");
        assertFalse(second.acquired());
        assertEquals(CoordinationErrorCode.LOCK_CONTENDED.name(), second.reason());
        log.info("[scenario-竞争] clientB 加锁被拒 errorCode={} (判定依据: 他人租约仍有效，区别于租约过期/令牌过旧)",
                second.reason());
    }

    @Test
    void expiredLeaseRejectsLateWrite_butRenewalWithinTtlWorks() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        LockService locks = cluster.lockService();

        LockHandle handle = locks.tryAcquire(leader, "order-2", "clientA").handle();
        FenceToken token = handle.token();

        // TTL 之内续期：成功
        cluster.advance(cluster.getLeaseTtlMillis() - 5);
        AcquireResult renewed = locks.renew(leader, token);
        assertTrue(renewed.acquired(), "TTL 内续期应当成功");
        log.info("[scenario-租约] 栅栏令牌={} 幂等键=order-2 在 TTL 内续期成功 新到期={} (判定依据: 令牌匹配且未过期)",
                token, renewed.handle().expireAtMillis());

        // 再推进到租约过期（续期把到期时间推后，因此要超过 TTL）
        cluster.advance(cluster.getLeaseTtlMillis() + 1);
        assertFalse(locks.isLocked("order-2", cluster.clock().nowMillis()));

        CoordinationException lateWrite = assertThrows(CoordinationException.class,
                () -> locks.validateWriteToken("order-2", token, cluster.clock().nowMillis()));
        assertEquals(CoordinationErrorCode.LEASE_EXPIRED, lateWrite.getErrorCode());
        log.info("[scenario-租约] 旧持有者迟到写被拒 errorCode={} 栅栏令牌={} 幂等键=order-2 (判定依据: now>expireAt)",
                lateWrite.getErrorCode(), token);

        AcquireResult lateRenew = locks.renew(leader, token);
        assertEquals(CoordinationErrorCode.LEASE_EXPIRED.name(), lateRenew.reason());
    }

    @Test
    void staleFenceTokenRejected_afterNewTermOwnerTakesOver() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leader1Id = cluster.awaitLeader();
        RaftNode leader1 = cluster.node(leader1Id);
        LockService locks = cluster.lockService();

        LockHandle old = locks.tryAcquire(leader1, "order-3", "clientA").handle();
        FenceToken oldToken = old.token();
        long oldTerm = leader1.getCurrentTerm();
        log.info("[scenario-令牌] 旧锁 栅栏令牌={} term={} 幂等键=order-3", oldToken, oldTerm);

        // 旧领导者被隔离并过期，多数派选出新任期领导者
        cluster.isolate(leader1Id);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String leader2Id = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(leader2Id);
        long newTerm = leader2.getCurrentTerm();
        assertNotEquals(leader1Id, leader2Id);
        assertTrue(newTerm > oldTerm, "新任期必须更大");

        // 旧锁租约已过期，clientB 在新领导者处获得新锁，栅栏令牌严格变大
        cluster.advance(2);
        LockHandle fresh = locks.tryAcquire(leader2, "order-3", "clientB").handle();
        FenceToken newToken = fresh.token();
        assertTrue(newToken.compareTo(oldToken) > 0,
                "栅栏令牌必须随任期单调递增: new=" + newToken + " old=" + oldToken);
        log.info("[scenario-令牌] 新锁 栅栏令牌={} term={} 幂等键=order-3 (判定依据: 新 term {} > 旧 term {}，令牌严格递增)",
                newToken, newTerm, newTerm, oldTerm);

        // 旧持有者迟到写（锁记录已被更高令牌覆盖）：必须判定为 STALE_FENCE_TOKEN
        CoordinationException stale = assertThrows(CoordinationException.class,
                () -> locks.validateWriteToken("order-3", oldToken, cluster.clock().nowMillis()));
        assertEquals(CoordinationErrorCode.STALE_FENCE_TOKEN, stale.getErrorCode());
        log.info("[scenario-令牌] 旧持有者迟到写被拒 errorCode={} 旧令牌={} 当前令牌={} (判定依据: 栅栏代次已前进)",
                stale.getErrorCode(), oldToken, newToken);

        // 新持有者在租约内的写必须通过
        locks.validateWriteToken("order-3", newToken, cluster.clock().nowMillis());

        // 旧持有者拿旧令牌释放也应失败，不能误释放新持有者的锁
        AcquireResult release = locks.release(leader2, oldToken);
        assertFalse(release.acquired());
        assertEquals(CoordinationErrorCode.STALE_FENCE_TOKEN.name(), release.reason());
        assertTrue(locks.isLocked("order-3", cluster.clock().nowMillis()),
                "旧令牌释放不得影响新持有者");
    }

    @Test
    void nonLeaderCannotIssueLocks() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode follower = cluster.nodes().stream()
                .filter(n -> !n.getNodeId().equals(leaderId)).findFirst().orElseThrow();
        AcquireResult result = cluster.lockService().tryAcquire(follower, "order-4", "clientX");
        assertFalse(result.acquired());
        assertEquals(CoordinationErrorCode.NOT_LEADER.name(), result.reason());
        log.info("[scenario-角色] follower 加锁被拒 errorCode={} (判定依据: 非领导者不颁发锁)", result.reason());
    }
}
