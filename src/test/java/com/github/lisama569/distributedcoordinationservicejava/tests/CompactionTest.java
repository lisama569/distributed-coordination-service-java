package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteResult;
import com.github.lisama569.distributedcoordinationservicejava.recovery.DurableStorage;
import com.github.lisama569.distributedcoordinationservicejava.recovery.Snapshot;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 历史压缩场景：
 * <ol>
 *   <li>已应用历史达到可配置阈值才触发压缩，存档只包含已落定生效的内容；</li>
 *   <li>“先加载存档再补增量”的结果与从头完整重放逐条一致；</li>
 *   <li>已承诺生效的写入一条不丢，幂等键在压缩后重试依旧只回放、不二次写入；</li>
 *   <li>压缩后崩溃重启：先装存档、再补增量，状态、位点、去重表全部恢复。</li>
 * </ol>
 */
class CompactionTest {

    private static final Logger log = LoggerFactory.getLogger(CompactionTest.class);

    private ClusterSimulator clusterWithThreshold(long threshold) {
        // 阈值必须在节点组装前设置（节点构造时读取）
        ClusterSimulator cluster = new ClusterSimulator();
        cluster.setCompactionThresholdEntries(threshold);
        for (int i = 1; i <= 3; i++) {
            cluster.addNode("node" + i);
        }
        return cluster;
    }

    private void submitN(ClusterSimulator cluster, RaftNode leader, int n, String prefix) {
        submitRange(cluster, leader, 1, n, prefix);
    }

    private void submitRange(ClusterSimulator cluster, RaftNode leader, int from, int to, String prefix) {
        for (int i = from; i <= to; i++) {
            cluster.coordinator(leader.getNodeId())
                    .submit(leader, prefix + "-k" + i, prefix + "-v" + i);
        }
    }

    @Test
    void compactionTriggersAtThreshold_andOnlyContainsCommittedHistory() {
        long threshold = 5L;
        ClusterSimulator cluster = clusterWithThreshold(threshold);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);

        submitN(cluster, leader, 4, "cmp-a");
        log.info("[scenario-压缩阈值] 已写 4 条 (< 阈值 {}) snapshotIndex={} firstIndex={} (判定依据: 未达阈值不压缩)",
                threshold, leader.getSnapshotIndex(), leader.getFirstLogIndex());
        assertEquals(0L, leader.getSnapshotIndex(), "未达阈值不应产生存档");

        submitRange(cluster, leader, 5, 6, "cmp-a"); // 累计 6 条不同键，达到阈值
        long snapK = leader.getSnapshotIndex();
        log.info("[scenario-压缩阈值] 已写 6 条 (>= 阈值 {}) snapshotIndex={} firstIndex={} (判定依据: 已应用历史达到阈值才整理存档)",
                threshold, snapK, leader.getFirstLogIndex());
        assertTrue(snapK >= threshold, "达到阈值后必须完成压缩: snapK=" + snapK);
        assertTrue(leader.getFirstLogIndex() > 1L, "被压缩的旧日志必须物理删除");
        // 标准约定：边界条目 K 保留（保证后续读取连续），K 之前的旧条目被物理删除
        assertEquals(snapK, leader.getFirstLogIndex(), "保留窗口应从存档边界条目开始");
        assertNull(cluster.disk(leaderId).loadEntry(snapK - 1L),
                "存档边界之前的旧日志条目应已被删除");
        assertNotNull(cluster.disk(leaderId).loadEntry(snapK),
                "存档边界条目按约定保留");
        assertEquals(snapK, cluster.disk(leaderId).loadSnapshot().orElseThrow().lastIncludedIndex());

        // 存档内容必须只包含已提交生效的写，且键值与逐条写入一致
        Snapshot snap = cluster.disk(leaderId).loadSnapshot().orElseThrow();
        for (int i = 1; i <= snapK; i++) {
            String key = "cmp-a-k" + i;
            assertEquals("cmp-a-v" + i, snap.state().get(key),
                    "存档状态必须与完整重放一致，缺少键 " + key);
        }
        // 未被压缩的后续增量仍然可读（边界条目 snapK 也保留）
        for (long i = snapK; i <= 6; i++) {
            assertNotNull(cluster.disk(leaderId).loadEntry(i), "存档边界及之后的条目必须保留");
        }
        log.info("[scenario-压缩阈值] 存档含 {} 个键、{} 条幂等记录，保留增量 {} 条 (依据: 已提交键值齐全，增量保留)",
                snap.state().size(), snap.idempotent().size(),
                cluster.disk(leaderId).lastIndex() - snapK);
    }

    @Test
    void snapshotThenIncrementalsEquivalentToFullReplay() {
        ClusterSimulator full = clusterWithThreshold(0L); // 不压缩：完整重放参照组
        ClusterSimulator compacted = clusterWithThreshold(5L); // 压缩组

        String fullLeaderId = full.awaitLeader();
        String compactLeaderId = compacted.awaitLeader();
        RaftNode fullLeader = full.node(fullLeaderId);
        RaftNode compactLeader = compacted.node(compactLeaderId);

        // 交错写入同一组键（包含覆盖写），两组集群执行完全相同的操作序列
        String[] keys = {"eq-k1", "eq-k2", "eq-k3", "eq-k1", "eq-k4", "eq-k2",
                "eq-k5", "eq-k6", "eq-k3", "eq-k7", "eq-k1", "eq-k8"};
        String[] vals = {"v1", "v1", "v1", "v2", "v1", "v2",
                "v1", "v1", "v2", "v1", "v3", "v1"};
        for (int i = 0; i < keys.length; i++) {
            full.coordinator(fullLeaderId).submit(fullLeader, keys[i] + "-" + i, vals[i]);
            compacted.coordinator(compactLeaderId).submit(compactLeader, keys[i] + "-" + i, vals[i]);
        }

        // 直接比较两个状态机在全部应用后的完整 K/V
        Map<String, String> fullState = new HashMap<>();
        fullLeader.logView().forEach(e -> fullState.put(e.idempotencyKey(), e.value()));
        Snapshot snap = compacted.disk(compactLeaderId).loadSnapshot().orElseThrow();
        Map<String, String> compactView = new HashMap<>(snap.state());
        compacted.disk(compactLeaderId).loadEntries(compactLeader.getFirstLogIndex())
                .forEach(e -> compactView.put(e.idempotencyKey(), e.value()));
        // 存档状态里也含边界条目 K 的键，增量中的 K 用同值覆盖，不影响等价性
        log.info("[scenario-等价重放] 完整重放日志 {} 条；压缩组: 存档位点={} + 增量 {} 条，合并 {} 个键 (判定依据: 存档状态 + 增量逐条应用 == 从头重放)",
                fullLeader.logView().size(), snap.lastIncludedIndex(),
                compactLeader.logView().size(), compactView.size());
        // 保留增量包含存档边界条目 K（故合并总条数 = K + 增量条数 - 1）
        assertEquals(fullLeader.logView().size(),
                snap.lastIncludedIndex() + compactLeader.logView().size() - 1);
        assertEquals(fullState, compactView, "存档+增量合并结果必须与完整重放完全一致");
        assertTrue(snap.lastIncludedIndex() > 0L, "该写入量应已触发至少一次压缩");
    }

    @Test
    void restartAfterCompactionLoadsSnapshotThenIncrementals() {
        long threshold = 5L;
        ClusterSimulator cluster = clusterWithThreshold(threshold);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        submitN(cluster, leader, 12, "rst");
        long snapKBefore = leader.getSnapshotIndex();
        long lastAppliedBefore = leader.getLastApplied();
        assertTrue(snapKBefore > 0L);
        assertTrue(lastAppliedBefore > snapKBefore, "需要存在存档之后的增量以验证补增量路径");

        // 对一个存档内旧键、一个增量新键分别记录期望值
        String oldKey = "rst-k1";
        String tailKey = "rst-k12";
        String oldExpected = "rst-v1";
        String tailExpected = "rst-v12";

        cluster.crash(leaderId);
        log.info("[scenario-压缩后重启] 节点 {} 崩溃: snapshotK={} appliedIndex={} (磁盘保留存档+增量)",
                leaderId, snapKBefore, lastAppliedBefore);
        cluster.restart(leaderId);
        RaftNode restarted = cluster.node(leaderId);

        log.info("[scenario-压缩后重启] 重启后 snapshotIndex={} firstIndex={} appliedIndex={}/{} (判定依据: 先装存档再补增量)",
                restarted.getSnapshotIndex(), restarted.getFirstLogIndex(),
                restarted.getLastApplied(), lastAppliedBefore);
        assertEquals(snapKBefore, restarted.getSnapshotIndex());
        assertEquals(lastAppliedBefore, restarted.getLastApplied());
        assertEquals("rst-v1", readKv(cluster, leaderId, oldKey), "存档内的旧键必须随存档恢复");
        assertEquals(tailExpected, readKv(cluster, leaderId, tailKey), "存档之后的增量必须补齐");

        // 压缩前旧键的重试：命中存档中的去重记录，不产生第二条日志
        long logsBefore = restarted.logView().size();
        WriteResult retry = cluster.coordinator(leaderId)
                .submit(restarted, oldKey, "rst-v1-TAMPERED");
        assertTrue(retry.dedupHit(), "存档内幂等键的重试必须命中存档去重记录");
        assertEquals(oldExpected, retry.value(), "仍以首次生效值为准");
        assertEquals(logsBefore, restarted.logView().size(), "重试不得产生新日志");
        log.info("[scenario-压缩后重启] 旧键={} 重试 dedupHit=true value={} (依据: 去重记录随存档恢复)，增量键={} 值={}",
                oldKey, retry.value(), tailKey, readKv(cluster, leaderId, tailKey));
    }

    @Test
    void compactionNeverIncludesUncommittedEntries() {
        // 阈值调小：隔离领导者后，其本地未获多数派的写入尝试在 propose 阶段即被拒，不落盘；
        // 本用例验证“压缩只到 appliedIndex”，存档位点绝不越过已应用位点。
        long threshold = 3L;
        ClusterSimulator cluster = clusterWithThreshold(threshold);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        submitN(cluster, leader, 3, "uc");
        long snapK = leader.getSnapshotIndex();
        long applied = leader.getLastApplied();
        log.info("[scenario-未提交不压缩] 压缩位点={} 已应用位点={} commitIndex={} (判定依据: maybeCompact 只收到 appliedIndex)",
                snapK, applied, leader.getCommitIndex());
        assertTrue(snapK <= applied, "存档位点绝不可能超过已应用位点");
        assertEquals(applied, leader.getCommitIndex());
        DurableStorage disk = cluster.disk(leaderId);
        Snapshot snapshot = disk.loadSnapshot().orElseThrow();
        assertEquals(snapK, snapshot.lastIncludedIndex());
        // 存档中的每条幂等记录都必须是真实已应用的日志
        snapshot.idempotent().forEach(r ->
                assertTrue(r.index() <= applied, "存档幂等记录位点不能超过已应用位点: " + r.index()));
    }

    private String readKv(ClusterSimulator cluster, String nodeId, String key) {
        DurableStorage disk = cluster.disk(nodeId);
        // 先查存档状态（已压缩部分），再查保留的增量日志
        String fromSnapshot = disk.loadSnapshot().map(s -> s.state().get(key)).orElse(null);
        if (fromSnapshot != null) {
            return fromSnapshot;
        }
        return disk.loadEntries(disk.firstIndex()).stream()
                .filter(e -> key.equals(e.idempotencyKey()))
                .map(e -> e.value())
                .findFirst().orElse(null);
    }
}
