package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteCoordinator;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteResult;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Checkpoint;
import com.github.lisama569.distributedcoordinationservicejava.recovery.InMemoryDisk;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等与恢复场景：
 * <ol>
 *   <li>同一幂等键的首次提交 + 3 次重试（含不同值）只产生一条日志、一次写入效果；</li>
 *   <li>“并发/重复提交”以同 tick 内连续 5 次提交模拟，只有首次真正 propose；</li>
 *   <li>领导者崩溃 -> 用同一磁盘重启 -> 去重表恢复，重试仍不二次写入；</li>
 *   <li>任期回退、日志位点回退、应用位点回退均被显式拒绝（CHECKPOINT_ROLLBACK）。</li>
 * </ol>
 */
class IdempotencyRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyRecoveryTest.class);

    @Test
    void retriesAndDuplicatesProduceSingleWrite() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        WriteCoordinator coordinator = cluster.coordinator(leaderId);

        String key = "order-pay-1001";
        WriteResult first = coordinator.submit(leader, key, "amount=100");
        assertFalse(first.dedupHit());

        long logsBefore = leader.logView().size();
        WriteResult retry1 = coordinator.submit(leader, key, "amount=100");
        WriteResult retry2 = coordinator.submit(leader, key, "amount=999"); // 重试携带不同值
        WriteResult retry3 = coordinator.submit(leader, key, "amount=100");
        long logsAfter = leader.logView().size();

        log.info("[scenario-幂等] 幂等键={} 首次 index={} term={}；重试结论 dedupHit={}/{},{} (判定依据: 去重表命中即回放)",
                key, first.index(), first.term(),
                retry1.dedupHit(), retry2.dedupHit(), retry3.dedupHit());
        assertTrue(retry1.dedupHit() && retry2.dedupHit() && retry3.dedupHit());
        assertEquals(logsBefore, logsAfter, "重试不得产生新日志");
        assertEquals(first.index(), retry2.index());
        assertEquals("amount=100", retry2.value(), "首次提交值为准，重试值不得覆盖");
        assertEquals(1L, leader.logView().stream().filter(e -> key.equals(e.idempotencyKey())).count(),
                "同一幂等键在日志中只能出现一次");
    }

    @Test
    void concurrentDuplicateSubmissionsCollapseToOneEffect() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        WriteCoordinator coordinator = cluster.coordinator(leaderId);

        String key = "order-pay-2002";
        int writes = 0;
        int dedups = 0;
        // 同一时刻（无时钟推进）连续 5 次提交，模拟并发/重复请求交叠
        for (int i = 0; i < 5; i++) {
            WriteResult r = coordinator.submit(leader, key, "attempt=" + i);
            if (r.dedupHit()) {
                dedups++;
            } else {
                writes++;
            }
        }
        log.info("[scenario-并发重复] 幂等键={} 真正写入次数={} 去重次数={} 日志条数={} (判定依据: 提交前去重表检查)",
                key, writes, dedups, leader.logView().size());
        assertEquals(1, writes, "只允许一次真正写入");
        assertEquals(4, dedups);
        assertEquals(1L, leader.logView().stream().filter(e -> key.equals(e.idempotencyKey())).count());
    }

    @Test
    void crashAndRestartRestoresCheckpointWithoutReapplying() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        WriteCoordinator coordinator = cluster.coordinator(leaderId);

        coordinator.submit(leader, "order-3001", "v1");
        coordinator.submit(leader, "order-3002", "v2");
        long lastIndex = leader.getCommitIndex();
        long term = leader.getCurrentTerm();

        // 崩溃：内存对象丢弃
        cluster.crash(leaderId);
        log.info("[scenario-崩溃恢复] 领导者={} 崩溃，已持久化 lastIndex={} term={}", leaderId, lastIndex, term);

        // 重启：同一磁盘恢复
        cluster.restart(leaderId);
        RaftNode restarted = cluster.node(leaderId);
        WriteCoordinator restoredCoordinator = cluster.coordinator(leaderId);

        log.info("[scenario-崩溃恢复] 重启后 term={} lastApplied={} 去重记录恢复={}/{} (判定依据: 检查点+日志重放，状态机按位点去重)",
                restarted.getCurrentTerm(), restarted.getLastApplied(),
                restoredCoordinator.isCommitted("order-3001"), restoredCoordinator.isCommitted("order-3002"));
        assertEquals(term, restarted.getCurrentTerm(), "任期必须恢复到崩溃前值");
        assertEquals(lastIndex, restarted.getLastApplied(), "应用位点必须恢复");
        assertTrue(restoredCoordinator.isCommitted("order-3001"));
        assertTrue(restoredCoordinator.isCommitted("order-3002"));

        // 重启后重试旧幂等键：仍命中去重，不产生第二条日志
        WriteResult retry = restoredCoordinator.submit(restarted, "order-3001", "v1-retry");
        assertTrue(retry.dedupHit());
        assertEquals(1L, restarted.logView().stream()
                .filter(e -> "order-3001".equals(e.idempotencyKey())).count(),
                "恢复后重试不得重复应用");
        assertEquals(lastIndex, restarted.getCommitIndex(), "重试未产生新提交位点");
    }

    @Test
    void checkpointRollbackIsRejectedExplicitly() {
        InMemoryDisk disk = new InMemoryDisk();
        disk.saveCheckpoint(new Checkpoint(5L, "node1", 0L, 0L));

        CoordinationException termBack = assertThrows(CoordinationException.class,
                () -> disk.saveCheckpoint(new Checkpoint(4L, "node1", 0L, 0L)));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, termBack.getErrorCode());
        log.info("[scenario-回退] 任期回退被拒 errorCode={} 原因={}",
                termBack.getErrorCode(), termBack.getMessage());

        // 先制造一条 index=1 的日志再验证位点回退
        disk.appendEntry(new com.github.lisama569.distributedcoordinationservicejava.log.LogEntry(
                1L, 5L, "k", "v"));
        disk.saveCheckpoint(new Checkpoint(5L, "node1", 1L, 1L));
        CoordinationException indexBack = assertThrows(CoordinationException.class,
                () -> disk.saveCheckpoint(new Checkpoint(5L, "node1", 0L, 0L)));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, indexBack.getErrorCode());
        log.info("[scenario-回退] 日志位点回退被拒 errorCode={} 原因={}",
                indexBack.getErrorCode(), indexBack.getMessage());

        // 非连续追加同样拒绝
        CoordinationException gap = assertThrows(CoordinationException.class,
                () -> disk.appendEntry(new com.github.lisama569.distributedcoordinationservicejava.log.LogEntry(
                        3L, 5L, "k2", "v2")));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, gap.getErrorCode());
        log.info("[scenario-回退] 非连续日志追加被拒 errorCode={} 原因={}",
                gap.getErrorCode(), gap.getMessage());
    }

    @Test
    void stateMachineRejectsAppliedIndexRollback() {
        com.github.lisama569.distributedcoordinationservicejava.statemachine.KvStateMachine sm =
                new com.github.lisama569.distributedcoordinationservicejava.statemachine.KvStateMachine();
        sm.apply(1L, "k1", "v1");
        sm.apply(2L, "k2", "v2");
        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> sm.apply(1L, "k1", "old"));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, ex.getErrorCode());
        log.info("[scenario-回退] 状态机应用位点回退被拒 errorCode={} 原因={}",
                ex.getErrorCode(), ex.getMessage());
    }
@Test
    void staleLeaderLateWriteIsNeverPersisted_andReplicasSurviveLeaderCrash() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        cluster.coordinator(leaderId).submit(leader, "dup-safe-1", "a");
        cluster.coordinator(leaderId).submit(leader, "dup-safe-2", "b");

        // 隔离旧领导者并使其租约过期
        cluster.isolate(leaderId);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String newLeaderId = cluster.awaitLeader();
        RaftNode newLeader = cluster.node(newLeaderId);
        long oldTerm = leader.getCurrentTerm();
        assertTrue(newLeader.getCurrentTerm() > oldTerm);

        // 旧领导者的迟到写必须被拒绝，且绝不能在其本地日志里凭空多出一条
        long diskBefore = leader.logView().size();
        CoordinationException rejected = assertThrows(CoordinationException.class,
                () -> cluster.coordinator(leaderId).submit(leader, "dup-safe-3", "late"));
        assertTrue(rejected.getErrorCode() == CoordinationErrorCode.NOT_LEADER
                        || rejected.getErrorCode() == CoordinationErrorCode.LEASE_EXPIRED,
                "旧领导者迟到写必须被明确拒绝: " + rejected.getErrorCode());
        long diskAfter = leader.logView().size();
        log.info("[scenario-迟到写] 旧 term={} 的写被拒 errorCode={}，本地日志条数 {} -> {} (判定依据: 无有效租约/已下台，拒绝发生在落盘之前)",
                oldTerm, rejected.getErrorCode(), diskBefore, diskAfter);
        assertEquals(diskBefore, diskAfter, "被拒绝的迟到写绝不允许落盘");

        // 新任期在多数派一侧继续写入并复制
        cluster.coordinator(newLeaderId).submit(newLeader, "dup-safe-3", "new-term-write");

        // 新领导者崩溃：其余副本已持有全部已提交日志，重启（或在其他节点读）数据不丢
        long committedIndex = newLeader.getCommitIndex();
        cluster.crash(newLeaderId);
        cluster.restart(newLeaderId);
        RaftNode revived = cluster.node(newLeaderId);
        log.info("[scenario-崩溃恢复] 新领导者={} 崩溃重启 commitIndex={}/{} 去重键 dup-safe-3 命中={}",
                newLeaderId, revived.getCommitIndex(), committedIndex,
                cluster.coordinator(newLeaderId).isCommitted("dup-safe-3"));
        assertEquals(committedIndex, revived.getCommitIndex());
        assertTrue(cluster.coordinator(newLeaderId).isCommitted("dup-safe-3"));
        WriteResult replay = cluster.coordinator(newLeaderId)
                .submit(revived, "dup-safe-3", "should-be-ignored");
        assertTrue(replay.dedupHit());
        assertEquals("new-term-write", replay.value());
    }
}
