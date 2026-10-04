package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteResult;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Snapshot;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 滞后节点追赶场景：
 * <ol>
 *   <li>长时间失联 / 新加入节点靠“存档 + 后续增量”补齐并重新参与多数派；</li>
 *   <li>追赶期间集群照常接受新写入，不被堵住；</li>
 *   <li>同一次写在补齐过程中不会被算两遍（去重表存档 + 应用回调，按位点/键去重）；</li>
 *   <li>掉队太远、超出保留窗口的节点被明确挡回（BEYOND_RECOVERY_WINDOW），
 *       不假装追平、不从中间位点悄悄续传。</li>
 * </ol>
 */
class CatchUpTest {

    private static final Logger log = LoggerFactory.getLogger(CatchUpTest.class);

    private ClusterSimulator clusterWithThreshold(long threshold) {
        ClusterSimulator cluster = new ClusterSimulator();
        cluster.setCompactionThresholdEntries(threshold);
        for (int i = 1; i <= 3; i++) {
            cluster.addNode("node" + i);
        }
        return cluster;
    }

    private void submitN(ClusterSimulator cluster, RaftNode leader, int from, int to, String prefix) {
        for (int i = from; i <= to; i++) {
            cluster.coordinator(leader.getNodeId())
                    .submit(leader, prefix + "-k" + i, prefix + "-v" + i);
        }
    }

    private String readKv(ClusterSimulator cluster, String nodeId, String key) {
        var disk = cluster.disk(nodeId);
        String fromSnap = disk.loadSnapshot().map(s -> s.state().get(key)).orElse(null);
        if (fromSnap != null) {
            return fromSnap;
        }
        return disk.loadEntries(disk.firstIndex()).stream()
                .filter(e -> key.equals(e.idempotencyKey())).map(e -> e.value())
                .findFirst().orElse(null);
    }

    /** 驱动若干个心跳 tick，让领导者把存档/增量推给落后节点。 */
    private void pumpHeartbeats(ClusterSimulator cluster, int ticks) {
        for (int i = 0; i < ticks; i++) {
            cluster.advance(cluster.getHeartbeatIntervalMillis() + 1L);
        }
    }

    @Test
    void newNodeCatchesUpViaSnapshotThenIncrementals_andRejoinsQuorum() {
        long threshold = 5L;
        ClusterSimulator cluster = clusterWithThreshold(threshold);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);

        // 压缩发生后再让全新节点加入：它本地没有任何历史，只能先吃存档
        submitN(cluster, leader, 1, 8, "join");
        long leaderSnapK = leader.getSnapshotIndex();
        assertTrue(leaderSnapK > 0L, "前置：集群已发生压缩");
        log.info("[scenario-新节点追赶] 写入 8 条后领导者存档位点={}，新节点空白加入", leaderSnapK);

        cluster.joinNewNode("node4");
        // 加入后立即继续写：追赶期间集群不能停写
        submitN(cluster, leader, 9, 11, "join");
        pumpHeartbeats(cluster, 3);
        // 追赶期间再写一批，验证不阻塞
        submitN(cluster, leader, 12, 13, "join");
        pumpHeartbeats(cluster, 3);

        RaftNode newcomer = cluster.node("node4");
        long leaderLast = leader.getLastApplied();
        log.info("[scenario-新节点追赶] 追赶后 node4 snapshotIndex={} appliedIndex={}/{} firstIndex={} (判定依据: 先装存档再补增量)",
                newcomer.getSnapshotIndex(), newcomer.getLastApplied(), leaderLast, newcomer.getFirstLogIndex());
        assertEquals(leaderLast, newcomer.getLastApplied(), "新节点必须补齐到领导者最新位点");
        assertEquals("join-v1", readKv(cluster, "node4", "join-k1"), "存档内历史键必须随存档到位");
        assertEquals("join-v13", readKv(cluster, "node4", "join-k13"), "追赶期间的最新写入必须到位");

        // 新节点补齐后应能参与多数派：隔离原领导者，剩余 3 个节点（含 node4）能选出新领导者
        cluster.isolate(leaderId);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String newLeaderId = cluster.awaitLeader();
        log.info("[scenario-新节点追赶] 隔离旧领导者后新领导者={}（node4 已重新参与多数派）", newLeaderId);
        RaftNode newLeader = cluster.node(newLeaderId);
        assertNotNull(newLeader);
        assertEquals("join-v13", readKv(cluster, newLeaderId, "join-k13"));
        assertTrue(newLeader.getCurrentTerm() >= leader.getCurrentTerm());
    }

    @Test
    void writesContinueDuringCatchup_andNoWriteCountedTwice() {
        long threshold = 4L;
        ClusterSimulator cluster = clusterWithThreshold(threshold);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        submitN(cluster, leader, 1, 6, "flow");
        long snapK = leader.getSnapshotIndex();
        assertTrue(snapK > 0L);

        // 让一个旧 follower 长期失联：隔离期间集群继续压缩、继续写
        String laggard = "node3";
        cluster.isolate(laggard);
        submitN(cluster, leader, 7, 12, "flow"); // 期间继续写入，不被追赶阻塞
        log.info("[scenario-追赶不阻塞] {} 失联期间又写入 6 条，领导者存档位点={} lastApplied={}",
                laggard, leader.getSnapshotIndex(), leader.getLastApplied());

        cluster.healPartition();
        pumpHeartbeats(cluster, 4);
        RaftNode back = cluster.node(laggard);
        log.info("[scenario-追赶不阻塞] {} 恢复后 snapshotIndex={} appliedIndex={}/{} (判定依据: 存档覆盖被压缩历史，增量补齐其余)",
                laggard, back.getSnapshotIndex(), back.getLastApplied(), leader.getLastApplied());
        assertEquals(leader.getLastApplied(), back.getLastApplied());

        // 同一次写不能算两遍：存档键与增量键在去重表中各只有一条，
        // 重试任一已生效键都只回放首次结论
        for (String key : new String[]{"flow-k1", "flow-k5", "flow-k10", "flow-k12"}) {
            WriteResult r = cluster.coordinator(laggard).submit(back, key, "TAMPERED");
            assertTrue(r.dedupHit(), "追赶补齐后重试键 " + key + " 必须命中去重");
            long occurrences = cluster.disk(laggard).loadEntries(cluster.disk(laggard).firstIndex())
                    .stream().filter(e -> key.equals(e.idempotencyKey())).count();
            long inSnapshot = cluster.disk(laggard).loadSnapshot()
                    .map(s -> s.idempotent().stream().filter(r2 -> r2.idempotencyKey().equals(key)).count())
                    .orElse(0L);
            assertEquals(1L, occurrences + inSnapshot, "同一次写在存档与增量中合计只能出现一次: " + key);
        }
        log.info("[scenario-追赶不阻塞] 4 个键重试均 dedupHit=true，存档/增量中均只出现一次 (依据: 存档去重记录 + 按位点应用回调)");
    }

    @Test
    void nodeBeyondRetentionWindowIsRejectedExplicitly() {
        long threshold = 5L;
        // 4 节点：node4 仅短暂参与、只拿到前 2 条原始日志（无任何存档），随后长期失联
        ClusterSimulator cluster = new ClusterSimulator();
        cluster.setCompactionThresholdEntries(threshold);
        for (int i = 1; i <= 4; i++) {
            cluster.addNode("node" + i);
        }
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        submitN(cluster, leader, 1, 2, "win");
        pumpHeartbeats(cluster, 1);
        String victim = "node4";
        assertEquals(2L, cluster.node(victim).getLastApplied(), "前置：node4 只停在 index=2");
        assertEquals(0L, cluster.node(victim).getSnapshotIndex(), "前置：node4 没有任何存档");

        // node4 长期失联（崩溃），集群在多数派上继续压缩、继续写入
        cluster.crash(victim);
        submitN(cluster, leader, 3, 14, "win");
        long leaderK = cluster.node(leaderId).getSnapshotIndex();
        assertTrue(leaderK > 2L, "集群存档已推进过 node4 的位点: K=" + leaderK);

        // 模拟 node4 离线太久，本地仅剩的极早历史也已出现无法被存档覆盖的空洞：
        // 它磁盘上没有存档，只有 index=2 一条（index=1 被保留策略物理回收）。
        cluster.simulateOfflineRetentionGc(victim, 1L);
        log.info("[scenario-超窗挡回] 掉队节点 {} 仅残留 index=2 且无存档；集群存档位点={} 已跨过其全部历史",
                victim, leaderK);

        // 重启恢复必须立刻识别出无法跨越的历史空洞：显式 BEYOND_RECOVERY_WINDOW，
        // 不允许静默跳过、假装追平或从中间位点续传。
        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> cluster.restart(victim));
        assertEquals(CoordinationErrorCode.BEYOND_RECOVERY_WINDOW, ex.getErrorCode(),
                "掉队太远必须在恢复时被明确挡回: " + ex.getMessage());
        log.info("[scenario-超窗挡回] 超出可恢复窗口被明确拒绝 errorCode={} 原因={} (判定依据: 所需历史早于保留窗口且本地无存档，禁止从中间位点续传)",
                ex.getErrorCode(), ex.getMessage());
    }

    @Test
    void staleSnapshotCannotOverwriteNewerState() {
        // 直接在磁盘层验证：更旧的存档不能覆盖较新的存档（防止旧任期/旧位点借追赶降级）
        var disk = new com.github.lisama569.distributedcoordinationservicejava.recovery.InMemoryDisk();
        disk.appendEntry(new com.github.lisama569.distributedcoordinationservicejava.log.LogEntry(1L, 1L, "a", "1"));
        disk.appendEntry(new com.github.lisama569.distributedcoordinationservicejava.log.LogEntry(2L, 1L, "b", "2"));
        var newer = new Snapshot(2L, 1L, java.util.Map.of("a", "1", "b", "2"), java.util.List.of());
        disk.installSnapshot(newer);
        var older = new Snapshot(1L, 1L, java.util.Map.of("a", "1"), java.util.List.of());
        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> disk.installSnapshot(older));
        assertEquals(CoordinationErrorCode.CHECKPOINT_ROLLBACK, ex.getErrorCode());
        log.info("[scenario-旧存档拒绝] 更旧存档(K=1)覆盖新存档(K=2)被拒 errorCode={} 原因={}",
                ex.getErrorCode(), ex.getMessage());
    }
}
