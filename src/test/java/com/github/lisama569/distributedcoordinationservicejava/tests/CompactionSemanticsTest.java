package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteResult;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.lock.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.lock.LockHandle;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 压缩/追赶与原有协调语义的边界保持：
 * <ol>
 *   <li>不管是否压缩、是否换过领导者，同一幂等键重试只产生一次写入效果（含换主后重试）；</li>
 *   <li>仍处于有效租约内的锁持有关系不被压缩中断；</li>
 *   <li>过期/令牌落后的写依旧被拒，且与竞争失败可区分；</li>
 *   <li>压缩可能发生在请求之前或之后，结论一致。</li>
 * </ol>
 */
class CompactionSemanticsTest {

    private static final Logger log = LoggerFactory.getLogger(CompactionSemanticsTest.class);

    private ClusterSimulator clusterWithThreshold(long threshold) {
        ClusterSimulator cluster = new ClusterSimulator();
        cluster.setCompactionThresholdEntries(threshold);
        for (int i = 1; i <= 3; i++) {
            cluster.addNode("node" + i);
        }
        return cluster;
    }

    @Test
    void idempotentKeyAppliedOnceAcrossCompactionAndLeaderChange() {
        long threshold = 4L;
        ClusterSimulator cluster = clusterWithThreshold(threshold);
        String leader1Id = cluster.awaitLeader();
        RaftNode leader1 = cluster.node(leader1Id);

        String key = "pay-across-term";
        WriteResult first = cluster.coordinator(leader1Id).submit(leader1, key, "amount=100");
        long firstIndex = first.index();
        long firstTerm = first.term();
        log.info("[scenario-换主幂等] 首次提交 term={} index={} 幂等键={}", firstTerm, firstIndex, key);

        // 制造多轮压缩（请求之后压缩），并隔离领导者触发换主
        for (int i = 1; i <= 9; i++) {
            cluster.coordinator(leader1Id).submit(leader1, "filler-a-" + i, "x" + i);
        }
        cluster.isolate(leader1Id);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String leader2Id = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(leader2Id);
        assertNotEquals(leader1Id, leader2Id);
        long term2 = leader2.getCurrentTerm();
        assertTrue(term2 > firstTerm);
        log.info("[scenario-换主幂等] 换主: {} term={} -> {} term={}（旧领导者已压缩多轮）",
                leader1Id, firstTerm, leader2Id, term2);

        // 新领导者任期内继续写入并再次压缩（请求之前压缩）
        for (int i = 1; i <= 6; i++) {
            cluster.coordinator(leader2Id).submit(leader2, "filler-b-" + i, "y" + i);
        }
        long newSnapK = leader2.getSnapshotIndex();
        assertTrue(newSnapK >= firstIndex, "首次写入应已落入新领导者存档");

        // 在新领导者上用旧幂等键重试（值还故意不同）：必须只回放首次结论
        WriteResult retry = cluster.coordinator(leader2Id).submit(leader2, key, "amount=999");
        log.info("[scenario-换主幂等] 换主后重试幂等键={} dedupHit={} term={}/index={} value={} (判定依据: 新领导者从存档+增量重建的去重表命中首次结论)",
                key, retry.dedupHit(), retry.term(), retry.index(), retry.value());
        assertTrue(retry.dedupHit(), "换主后重试必须命中去重（去重结论随存档复制）");
        assertEquals(firstIndex, retry.index());
        assertEquals(firstTerm, retry.term());
        assertEquals("amount=100", retry.value(), "必须以首次生效值为准");

        // 全集群该幂等键只出现一次（新领导者保留日志 + 存档记录合计）
        var disk = cluster.disk(leader2Id);
        long inLog = disk.loadEntries(disk.firstIndex()).stream()
                .filter(e -> key.equals(e.idempotencyKey())).count();
        long inSnap = disk.loadSnapshot().map(s -> s.idempotent().stream()
                .filter(r -> r.idempotencyKey().equals(key)).count()).orElse(0L);
        assertEquals(1L, inLog + inSnap, "跨任期/跨压缩同一幂等键合计只能出现一次");

        // 旧领导者分区恢复后：旧键重试仍只回放首次结论（去重先于提议）；
        // 而新键写入必须以 NOT_LEADER 被拒，不能二次写入
        cluster.healPartition();
        cluster.advance(cluster.getHeartbeatIntervalMillis() + 1);
        RaftNode old = cluster.node(leader1Id);
        WriteResult replayOnOld = cluster.coordinator(leader1Id).submit(old, key, "amount=777");
        assertTrue(replayOnOld.dedupHit());
        assertEquals("amount=100", replayOnOld.value());
        CoordinationException rejected = assertThrows(CoordinationException.class,
                () -> cluster.coordinator(leader1Id).submit(old, "brand-new-key-after-heal", "z"));
        assertEquals(CoordinationErrorCode.NOT_LEADER, rejected.getErrorCode());
        log.info("[scenario-换主幂等] 旧领导者恢复后已下台：旧键重试仍只回放首次结论，新键写被拒 errorCode={}，幂等结论唯一",
                rejected.getErrorCode());
    }

    @Test
    void activeLeaseLockSurvivesCompaction() {
        long threshold = 3L;
        ClusterSimulator cluster = clusterWithThreshold(threshold);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);

        LockHandle handle = cluster.lockService().tryAcquire(leader, "order-lock", "clientA").handle();
        FenceToken token = handle.token();
        log.info("[scenario-锁跨压缩] 获锁 幂等键(lock)=order-lock 栅栏令牌={} 到期={}",
                token, handle.expireAtMillis());

        // 触发压缩（与锁完全正交），随后立刻校验：锁仍在、栅栏校验仍通过
        for (int i = 1; i <= 6; i++) {
            cluster.coordinator(leaderId).submit(leader, "lock-fill-" + i, "v" + i);
        }
        assertTrue(leader.getSnapshotIndex() > 0L, "期间必须发生过压缩");
        assertTrue(cluster.lockService().isLocked("order-lock", cluster.clock().nowMillis()),
                "有效租约内的锁持有关系不得被压缩中断");
        cluster.lockService().validateWriteToken("order-lock", token, cluster.clock().nowMillis());
        log.info("[scenario-锁跨压缩] 压缩至 snapshotIndex={} 后栅栏令牌={} 校验仍通过 (判定依据: 锁租约不参与日志压缩)",
                leader.getSnapshotIndex(), token);

        // TTL 内续期仍成功
        cluster.advance(cluster.getLeaseTtlMillis() - 5);
        assertTrue(cluster.lockService().renew(leader, token).acquired(), "压缩不影响 TTL 内续期");

        // 竞争者仍被明确判为 LOCK_CONTENDED（与租约/令牌错误可区分）
        var contention = cluster.lockService().tryAcquire(leader, "order-lock", "clientB");
        assertEquals(CoordinationErrorCode.LOCK_CONTENDED.name(), contention.reason());
        log.info("[scenario-锁跨压缩] 他者竞争 errorCode={}，与 LEASE_EXPIRED/STALE_FENCE_TOKEN 可区分",
                contention.reason());

        // TTL 过后迟到写仍判 LEASE_EXPIRED
        cluster.advance(cluster.getLeaseTtlMillis() + 1);
        CoordinationException expired = assertThrows(CoordinationException.class,
                () -> cluster.lockService().validateWriteToken("order-lock", token,
                        cluster.clock().nowMillis()));
        assertEquals(CoordinationErrorCode.LEASE_EXPIRED, expired.getErrorCode());
        log.info("[scenario-锁跨压缩] 租约过期后迟到写 errorCode={}（压缩不改变租约语义）",
                expired.getErrorCode());
    }

    @Test
    void staleFenceTokenStillRejectedAfterCompactionAndNewTerm() {
        long threshold = 3L;
        ClusterSimulator cluster = clusterWithThreshold(threshold);
        String leader1Id = cluster.awaitLeader();
        RaftNode leader1 = cluster.node(leader1Id);
        LockHandle old = cluster.lockService().tryAcquire(leader1, "fence-lock", "clientA").handle();

        cluster.isolate(leader1Id);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String leader2Id = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(leader2Id);
        // 新任期继续写入并压缩
        for (int i = 1; i <= 6; i++) {
            cluster.coordinator(leader2Id).submit(leader2, "fence-fill-" + i, "v" + i);
        }
        cluster.advance(2);
        LockHandle fresh = cluster.lockService().tryAcquire(leader2, "fence-lock", "clientB").handle();
        assertTrue(fresh.token().compareTo(old.token()) > 0);

        CoordinationException stale = assertThrows(CoordinationException.class,
                () -> cluster.lockService().validateWriteToken("fence-lock", old.token(),
                        cluster.clock().nowMillis()));
        assertEquals(CoordinationErrorCode.STALE_FENCE_TOKEN, stale.getErrorCode());
        log.info("[scenario-令牌跨压缩] 旧令牌={} 在新任期+压缩后迟到写 errorCode={} 当前令牌={} (判定依据: 栅栏代次单调，压缩不影响)",
                old.token(), stale.getErrorCode(), fresh.token());
    }
}
