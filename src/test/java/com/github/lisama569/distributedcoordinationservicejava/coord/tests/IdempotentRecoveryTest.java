package com.github.lisama569.distributedcoordinationservicejava.coord.tests;

import com.github.lisama569.distributedcoordinationservicejava.coord.client.CoordClient;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.FenceToken;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coord.recovery.Checkpoint;
import com.github.lisama569.distributedcoordinationservicejava.coord.recovery.FileCheckpointStore;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.Cluster;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 幂等与恢复测试：
 * 同一幂等键的重试与重复提交只有一次写入效果；崩溃重启从检查点恢复、不重复应用；
 * 任期/位点回退被显式拒绝。
 */
class IdempotentRecoveryTest extends AbstractCoordTest {

    @Test
    void retriesAndDuplicatesApplyExactlyOnce() {
        printSeparator("幂等：重试/重复提交只生效一次");
        Cluster cluster = newThreeNodeCluster();
        awaitLeaderId(cluster, 1000);
        CoordClient c = new CoordClient(cluster, "holder-A");

        FenceToken token = c.acquireLock("L", "acq", 500);
        c.fencedWrite("L", "k1", "v1", token, "idem-write-1");
        assertEquals("v1", c.read("k1"));

        // 用同一幂等键重复提交不同内容：只允许第一次生效，后续全部复用既有结论
        c.fencedWrite("L", "k1", "v2-DIFFERENT", token, "idem-write-1");
        c.fencedWrite("L", "k1", "v3-DIFFERENT", token, "idem-write-1");
        assertEquals("v1", c.read("k1"), "重复提交不得产生第二次写入效果");
        System.out.println("[断言] 同幂等键重复提交3次，KV 仍为首次值 v1");

        // 新幂等键 -> 新写入
        c.fencedWrite("L", "k1", "v2", token, "idem-write-2");
        assertEquals("v2", c.read("k1"));

        // 同幂等键的“拒绝结论”也必须稳定复用（锁竞争失败后重试，结论一致，不重复建锁）
        CoordClient other = new CoordClient(cluster, "holder-B");
        for (int i = 0; i < 2; i++) {
            CoordinationException ex = assertThrows(CoordinationException.class,
                    () -> other.acquireLock("L", "idem-busy"));
            assertEquals(RejectReason.LOCK_NOT_HELD, ex.getReason());
        }
        System.out.println("[断言] 竞争失败同键重试两次，结论稳定为 LOCK_NOT_HELD，锁未被重建");

        // 日志中必须能看到幂等命中记录
        assertFalse(eventsContaining("命中已提交结论").isEmpty(), "单测日志应打印幂等命中依据");
        assertFalse(eventsContaining("幂等键=idem-write-1").isEmpty(), "日志必须打印幂等键");
    }

    @Test
    void crashRestartRecoversWithoutReapplying() {
        printSeparator("恢复：领导者崩溃重启，从检查点恢复且不重复应用");
        FileCheckpointStore store = new FileCheckpointStore(checkpointDir);
        Cluster cluster = new Cluster(List.of("n1", "n2", "n3"), store,
                new com.github.lisama569.distributedcoordinationservicejava.coord.sim.ClusterConfig(
                        com.github.lisama569.distributedcoordinationservicejava.coord.raft.SimTimers.defaults(),
                        checkpointDir));
        awaitLeaderId(cluster, 1000);
        CoordClient c = new CoordClient(cluster, "holder-A");
        FenceToken token = c.acquireLock("L", "acq", 800);
        c.fencedWrite("L", "k1", "persisted-value", token, "idem-persist");
        String leaderId = cluster.leaderId();
        long termBefore = cluster.leader().getCurrentTerm();
        System.out.printf("[断言] 崩溃前领导者=%s 任期=%d, 已提交位点=%d%n",
                leaderId, termBefore, cluster.leader().getCommitIndex());

        // 领导者进程崩溃 -> 重启（从检查点恢复）
        cluster.crash(leaderId);
        RaftNode restarted = cluster.restart(leaderId);
        cluster.advance(30);
        long restoredCommit = restarted.getCommitIndex();
        System.out.printf("[断言] 重启后 %s: 角色=%s, 任期=%d, 提交位点=%d%n",
                leaderId, restarted.getRole(), restarted.getCurrentTerm(), restoredCommit);
        assertTrue(restoredCommit >= 2, "已提交条目必须从检查点恢复");
        // 重放只重建状态，不重复应用：幂等结论仍唯一
        assertNotNull(restarted.getIdempotentResult("idem-persist"));

        // 等待集群重新可用后，读取到的仍是崩溃前写入；同幂等键重试不会产生第二次效果
        assertTrue(cluster.advanceUntil(1500, () -> cluster.leader() != null));
        CoordClient c2 = new CoordClient(cluster, "holder-A");
        assertEquals("persisted-value", c2.read("k1"), "崩溃前已提交写入不得丢失");
        c2.fencedWrite("L", "k1", "SHOULD-NOT-APPLY", token, "idem-persist");
        assertEquals("persisted-value", c2.read("k1"), "恢复后重试旧幂等键不得重复应用");
        System.out.println("[断言] 重启后读取持久化值成功，旧幂等键重试未产生第二次效果");
        assertFalse(eventsContaining("从检查点恢复").isEmpty(), "日志必须打印检查点恢复依据");
    }

    @Test
    void rollbackCheckpointIsRejected() {
        printSeparator("恢复：任期/位点回退必须显式拒绝");
        FileCheckpointStore store = new FileCheckpointStore(checkpointDir);
        Cluster cluster = new Cluster(List.of("n1", "n2", "n3"), store,
                new com.github.lisama569.distributedcoordinationservicejava.coord.sim.ClusterConfig(
                        com.github.lisama569.distributedcoordinationservicejava.coord.raft.SimTimers.defaults(),
                        checkpointDir));
        awaitLeaderId(cluster, 1000);
        CoordClient c = new CoordClient(cluster, "holder-A");
        FenceToken token = c.acquireLock("L", "acq", 800);
        c.fencedWrite("L", "k", "v", token, "idem-rb-1");
        c.fencedWrite("L", "k2", "v2", token, "idem-rb-2");

        String leaderId = cluster.leaderId();
        Checkpoint newer = store.load(leaderId);
        assertNotNull(newer);
        long persistedTerm = newer.currentTerm;
        long persistedCommit = newer.commitIndex;
        System.out.printf("[断言] 已持久化检查点: 任期=%d 提交位点=%d%n", persistedTerm, persistedCommit);

        // 构造一份更旧的“回退”检查点：任期更小、提交位点更小
        Checkpoint stale = new Checkpoint();
        stale.nodeId = leaderId;
        stale.currentTerm = Math.max(0, persistedTerm - 1);
        stale.commitIndex = 0;
        stale.lastApplied = 0;
        stale.log = List.of();

        // 直接经 store 安全门：必须拒绝
        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> store.ensureNoRollback(leaderId, stale.currentTerm, 0, stale.commitIndex));
        assertEquals(RejectReason.ROLLBACK_REFUSED, ex.getReason());
        System.out.printf("[断言] 检查点回退被拒绝: 原因=%s, 信息=%s%n", ex.getReason(), ex.getMessage());

        // 节点从该旧检查点恢复同样被拒绝
        CoordinationException ex2 = assertThrows(CoordinationException.class,
                () -> cluster.node(leaderId).restoreFrom(stale));
        assertEquals(RejectReason.ROLLBACK_REFUSED, ex2.getReason());
        System.out.println("[断言] 节点 restoreFrom 旧检查点同样拒绝，拒绝继续执行");
        assertFalse(eventsContaining("拒绝任期/位点回退").isEmpty(), "日志必须打印回退拒绝依据");
    }
}
