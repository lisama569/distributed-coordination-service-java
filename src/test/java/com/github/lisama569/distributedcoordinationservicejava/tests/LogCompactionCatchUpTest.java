package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteResult;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.snapshot.Snapshot;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 历史压缩与滞后节点追赶场景：
 * <ol>
 *   <li>按规模阈值压缩后，状态/幂等索引与从头完整重放完全一致；旧日志前缀被删除；</li>
 *   <li>长时间失联节点（空盘重置）靠"存档 + 增量"补齐，追赶期间集群照常写入，
 *       已在集群生效的幂等键补齐后重试不产生第二次写入；</li>
 *   <li>压缩后崩溃重启：先装存档再补增量，状态、幂等结论不回归；</li>
 *   <li>换主后用旧幂等键重试：仍只回放、不二次写入；</li>
 *   <li>超出可恢复窗口的掉队节点被明确挡回（OUT_OF_RETENTION_WINDOW），
 *       不假装追平、不从中间位点续写；显式重置后才能以存档+增量重新加入。</li>
 * </ol>
 */
class LogCompactionCatchUpTest {

    private static final Logger log = LoggerFactory.getLogger(LogCompactionCatchUpTest.class);

    private ClusterSimulator clusterWithCompaction(int nodes, long threshold) {
        return new ClusterSimulator(nodes, threshold);
    }

    @Test
    void compactionProducesStateIdenticalToFullReplay_andDropsOldLogs() {
        ClusterSimulator cluster = clusterWithCompaction(3, 5L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);

        // 写 12 条已生效幂等写（阈值 5 -> 持续触发压缩）；同键重复提交以首次值为准
        for (int i = 1; i <= 12; i++) {
            cluster.coordinator(leaderId).submit(leader, "key-" + i, "value-" + i);
        }
        WriteResult duplicate = cluster.coordinator(leaderId).submit(leader, "key-3", "value-3-new");
        assertTrue(duplicate.dedupHit(), "同键重复提交必须回放首次结论");
        assertEquals("value-3", duplicate.value(), "首次提交值为准，重复值不得覆盖");

        long snap = leader.getSnapshotIndex();
        long applied = leader.getLastApplied();
        assertTrue(snap >= 5L, "写满阈值后必须已产生存档，实际 snapIndex=" + snap);
        assertEquals(applied, leader.getCommitIndex());

        // 被压缩的旧日志前缀不再保留
        assertNotNull(leader.logView());
        long firstRetained = snap + 1L;
        log.info("[scenario-压缩] 已应用={} 存档位点={} 保留增量起点={} 增量条数={} (判定依据: 阈值=5，仅压缩已提交应用位点)",
                applied, snap, firstRetained, leader.logView().size());

        // 存档 + 增量重放出来的状态必须与从头完整重放一致
        Map<String, String> expected = new java.util.LinkedHashMap<>();
        for (int i = 1; i <= 12; i++) {
            expected.put("key-" + i, "value-" + i);
        }
        for (Map.Entry<String, String> e : expected.entrySet()) {
            assertEquals(e.getValue(), leader.readState(e.getKey()),
                    "压缩后状态必须与完整重放一致: " + e.getKey());
        }
        // 存档之前的幂等键仍可命中（索引进了存档），不产生新日志
        WriteResult retry = cluster.coordinator(leaderId).submit(leader, "key-1", "retry-x");
        assertTrue(retry.dedupHit());
        assertEquals(1L, retry.index());
        assertEquals("value-1", retry.value());
        log.info("[scenario-压缩] 幂等键=key-1 压缩后重试 dedupHit={} index={} term={} 值={} (判定依据: 幂等索引随存档保留)",
                retry.dedupHit(), retry.index(), retry.term(), retry.value());
    }

    @Test
    void laggingNodeCatchesUpViaSnapshotAndDeltas_withoutBlockingWritesOrDoubleApply() {
        ClusterSimulator cluster = clusterWithCompaction(3, 5L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);

        // 选一个 follower 长时间隔离
        String laggard = cluster.nodes().stream()
                .map(RaftNode::getNodeId).filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();
        cluster.isolate(laggard);

        // 多数派一侧持续写入并多轮压缩，掉队者完全错过，且领导者存档推进到足够靠前
        for (int i = 1; i <= 22; i++) {
            cluster.coordinator(leaderId).submit(leader, "bulk-" + i, "v" + i);
        }
        // 追赶期间还要继续接受的新写入（先写一条用于追赶后幂等校验）
        cluster.coordinator(leaderId).submit(leader, "bulk-1", "v1-dup-should-replay");
        long leaderSnapBeforeHeal = leader.getSnapshotIndex();
        assertTrue(leaderSnapBeforeHeal >= 17L,
                "领导者存档应推进到覆盖大量历史，实际 snapIndex=" + leaderSnapBeforeHeal);
        log.info("[scenario-追赶] 掉队者={} 隔离期间集群写入 22 条并多轮压缩，领导者存档位点={}",
                laggard, leaderSnapBeforeHeal);

        // 掉队者磁盘陈旧：模拟"长时间失联回来"，按运维动作显式重置为空盘后重新加入
        cluster.healPartition();
        cluster.rejoinWithReset(laggard);

        // 追赶期间集群照常写入（多数派健康），不被追赶堵住
        for (int i = 23; i <= 25; i++) {
            WriteResult w = cluster.coordinator(leaderId).submit(leader, "bulk-" + i, "v" + i);
            assertTrue(w.committed(), "追赶期间新写入必须照常提交");
        }
        // 给心跳若干拍，让领导者把存档 + 增量推给掉队者
        for (int t = 0; t < 10; t++) {
            cluster.advance(cluster.getHeartbeatIntervalMillis());
        }

        RaftNode caught = cluster.node(laggard);
        log.info("[scenario-追赶] 补齐后 {} snapIndex={} appliedIndex={} 领导者 appliedIndex={}",
                laggard, caught.getSnapshotIndex(), caught.getLastApplied(), leader.getLastApplied());
        assertEquals(leader.getLastApplied(), caught.getLastApplied(),
                "存档+增量补齐后应用位点必须追平领导者");

        // 最终状态与从头重放一致：旧键、追赶期间新键都在
        for (int i = 1; i <= 25; i++) {
            assertEquals("v" + i, caught.readState("bulk-" + i),
                    "追赶补齐后状态必须一致: bulk-" + i);
        }
        // 已生效幂等键在补齐后的节点重试：只回放一次，不二次写入
        WriteResult replay = cluster.coordinator(laggard).submit(caught, "bulk-1", "again");
        assertTrue(replay.dedupHit(), "追赶补齐后旧幂等键必须命中已生效索引");
        assertEquals("v1", replay.value(), "以首次提交值为准");
        assertEquals(leader.getLastApplied(), caught.getLastApplied(), "幂等回放不得推进应用位点");
        log.info("[scenario-追赶] 幂等键=bulk-1 在补齐节点重试 dedupHit={} index={} (判定依据: 存档+增量尚存增量里的同一写，不重复 apply)",
                replay.dedupHit(), replay.index());
    }

    @Test
    void restartAfterCompactionLoadsSnapshotThenDeltas_withFullEquivalence() {
        ClusterSimulator cluster = clusterWithCompaction(3, 5L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);

        for (int i = 1; i <= 12; i++) {
            cluster.coordinator(leaderId).submit(leader, "restart-" + i, "rv" + i);
        }
        long snapBefore = leader.getSnapshotIndex();
        long appliedBefore = leader.getLastApplied();
        assertTrue(snapBefore > 0L, "前置：必须已经发生压缩");

        // 压缩后领导者崩溃：内存全丢，磁盘上保留存档 + 尾部增量
        cluster.crash(leaderId);
        log.info("[scenario-压缩后重启] {} 崩溃 snapIndex={} appliedIndex={}（磁盘保留存档+增量）",
                leaderId, snapBefore, appliedBefore);
        cluster.restart(leaderId);
        RaftNode revived = cluster.node(leaderId);

        log.info("[scenario-压缩后重启] 重启后 snapIndex={} appliedIndex={} (判定依据: 先装存档再补增量)",
                revived.getSnapshotIndex(), revived.getLastApplied());
        assertEquals(snapBefore, revived.getSnapshotIndex(), "存档位点必须恢复");
        assertEquals(appliedBefore, revived.getLastApplied(), "先装存档再补增量，应用位点必须追平崩溃前");

        // 状态与从头完整重放一致
        for (int i = 1; i <= 12; i++) {
            assertEquals("rv" + i, revived.readState("restart-" + i),
                    "压缩后重启状态必须一致: restart-" + i);
        }
        // 存档里的旧键与增量里的新键：重启后节点已应用索引（不依赖领导权）即可命中
        var oldRec = revived.findAppliedIdempotent("restart-1");
        var tailRec = revived.findAppliedIdempotent("restart-12");
        assertNotNull(oldRec);
        assertNotNull(tailRec);
        assertEquals("rv1", oldRec.value());
        assertEquals("rv12", tailRec.value());
        log.info("[scenario-压缩后重启] 旧键 restart-1(存档内)/尾键 restart-12(增量内) 重启后索引均可见，"
                        + "index={}/{} (判定依据: 存档幂等索引 + 增量日志共同重建)",
                oldRec.index(), tailRec.index());

        // 重启节点先以 follower 起来，集群重新选出有有效租约的领导者后继续写入与压缩
        String leader2Id = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(leader2Id);
        for (int i = 13; i <= 20; i++) {
            cluster.coordinator(leader2Id).submit(leader2, "restart-" + i, "rv" + i);
        }
        assertEquals("rv20", cluster.node(leader2Id).readState("restart-20"));
        assertTrue(cluster.node(leader2Id).getSnapshotIndex() >= snapBefore, "重启后应能继续压缩");
    }

    @Test
    void idempotentRetryAfterLeaderChange_stillSingleEffect() {
        ClusterSimulator cluster = clusterWithCompaction(3, 3L);
        String oldLeaderId = cluster.awaitLeader();
        RaftNode oldLeader = cluster.node(oldLeaderId);

        // 旧领导者写入并压缩
        WriteResult first = cluster.coordinator(oldLeaderId)
                .submit(oldLeader, "pay-9001", "amount=100");
        for (int i = 0; i < 6; i++) {
            cluster.coordinator(oldLeaderId).submit(oldLeader, "filler-" + i, "f" + i);
        }
        long oldTerm = oldLeader.getCurrentTerm();
        log.info("[scenario-换主重试] 旧主={} term={} 幂等键=pay-9001 首次 index={}，已压缩 snapIndex={}",
                oldLeaderId, oldTerm, first.index(), oldLeader.getSnapshotIndex());

        // 隔离旧主 -> 多数派选出更高 term 新主（新主已通过复制+压缩持有旧幂等索引）
        cluster.isolate(oldLeaderId);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String newLeaderId = cluster.awaitLeader();
        RaftNode newLeader = cluster.node(newLeaderId);
        assertTrue(newLeader.getCurrentTerm() > oldTerm);
        log.info("[scenario-换主重试] 新主={} term={}", newLeaderId, newLeader.getCurrentTerm());

        // 客户端拿着同一个幂等键找新主重试：必须回放首次结论，不产生第二次写入
        long appliedBefore = newLeader.getLastApplied();
        WriteResult retry = cluster.coordinator(newLeaderId)
                .submit(newLeader, "pay-9001", "amount=999");
        assertTrue(retry.dedupHit(), "换主后旧幂等键必须命中（索引随日志/存档复制到新主）");
        assertEquals(first.index(), retry.index());
        assertEquals("amount=100", retry.value());
        assertEquals(appliedBefore, newLeader.getLastApplied(), "重试不得推进应用位点");
        log.info("[scenario-换主重试] 幂等键=pay-9001 换主(term {}->{})后重试 dedupHit={} index={} term={} "
                        + "(判定依据: 已应用幂等索引随复制/压缩在新主可见)",
                oldTerm, newLeader.getCurrentTerm(), retry.dedupHit(), retry.index(), retry.term());
    }

    @Test
    void staleFollowerBeyondRetentionWindow_isRejectedExplicitly_thenResetRejoins() {
        ClusterSimulator cluster = clusterWithCompaction(3, 3L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        String stale = cluster.nodes().stream()
                .map(RaftNode::getNodeId).filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();

        // 先写一条并复制，让待隔离节点本地持久化"陈旧但真实"的历史
        cluster.coordinator(leaderId).submit(leader, "early-1", "e1");
        cluster.advance(cluster.getHeartbeatIntervalMillis());
        assertEquals("e1", cluster.node(stale).readState("early-1"));

        // 随后该节点失联；不推进逻辑时钟：每次写内部的多数派心跳都会续约，
        // 整个隔离窗口内同一节点始终是同一 term 的有效领导者
        cluster.isolate(stale);
        for (int i = 2; i <= 20; i++) {
            cluster.coordinator(leaderId).submit(leader, "early-" + i, "e" + i);
        }
        assertTrue(leader.isLeader() && leader.hasValidLease(cluster.clock().nowMillis()),
                "隔离期间领导者应靠每次写的多数派心跳持续续约");
        String stableLeaderId = leaderId;
        RaftNode stableLeader = leader;
        long leaderSnap = stableLeader.getSnapshotIndex();
        log.info("[scenario-超窗挡回] 多数派稳定领导者={} term={} snapIndex={}",
                stableLeaderId, stableLeader.getCurrentTerm(), leaderSnap);

        // 不重置、直接恢复连通：领导者推存档，掉队者因自带存档之外旧历史被明确挡回。
        // 掉队者隔离期间选举计时器已到期，恢复瞬间它可能先参选并把 term 抬高；无论谁当
        // 选，带着超窗旧历史的掉队者都会被在任领导者挡回。这里推进到出现稳定领导者，
        // 再向其断言挡回结论。
        cluster.healPartition();
        RaftNode serving = stableLeader;
        Map<String, String> blocked = Map.of();
        for (int t = 0; t < 60; t++) {
            cluster.advance(1L);
            RaftNode cur = cluster.leader();
            if (cur != null) {
                serving = cur;
                blocked = cur.blockedCatchUpPeers();
                if (blocked.containsKey(stale)) {
                    break;
                }
            }
        }
        assertTrue(serving.blockedCatchUpPeers().containsKey(stale),
                "掉队超窗节点必须被明确标记挡回，实际 blocked="
                        + serving.blockedCatchUpPeers().keySet());
        RaftNode staleNode = cluster.node(stale);
        assertTrue(staleNode.getLastApplied() < serving.getLastApplied(),
                "被挡回节点不允许假装追平");
        log.warn("[scenario-超窗挡回] {} 被在任领导者 {} 明确挡回: {} (判定依据: "
                        + "OUT_OF_RETENTION_WINDOW，不覆盖旧历史、不从中间位点续写)",
                stale, serving.getNodeId(), serving.blockedCatchUpPeers().get(stale));

        // 唯一合法恢复：显式重置本地历史 -> 存档+增量补齐
        cluster.rejoinWithReset(stale);
        for (int t = 0; t < 30; t++) {
            cluster.advance(cluster.getHeartbeatIntervalMillis());
            if (cluster.node(stale).getLastApplied() >= serving.getLastApplied()) {
                break;
            }
        }
        RaftNode rejoined = cluster.node(stale);
        assertEquals(serving.getLastApplied(), rejoined.getLastApplied(),
                "重置后必须靠存档+增量真正追平");
        assertEquals("e20", rejoined.readState("early-20"));
        assertFalse(serving.blockedCatchUpPeers().containsKey(stale),
                "成功补齐后挡回标记必须清除");
        log.info("[scenario-超窗挡回] {} 显式重置后以存档+增量追平 appliedIndex={}，重新正常参与多数派",
                stale, rejoined.getLastApplied());
    }

    @Test
    void activeLeaseLockSurvivesCompactionAndCatchUp() {
        ClusterSimulator cluster = clusterWithCompaction(3, 3L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);

        // 拿一把锁，随后进行大量写入触发多次压缩
        var acquire = cluster.lockService().tryAcquire(leader, "critical-resource", "owner-1");
        assertTrue(acquire.acquired());
        var token = acquire.handle().token();
        for (int i = 1; i <= 12; i++) {
            cluster.coordinator(leaderId).submit(leader, "work-" + i, "w" + i);
        }
        assertTrue(leader.getSnapshotIndex() > 0L, "期间必须发生过压缩");

        // 仍在租约 TTL 内：压缩不得中断持有关系，栅栏校验仍通过，迟到旧令牌语义不变
        cluster.lockService().validateWriteToken("critical-resource", token,
                cluster.clock().nowMillis());
        var renewed = cluster.lockService().tryAcquire(leader, "critical-resource", "owner-2");
        assertEquals(CoordinationErrorCode.LOCK_CONTENDED.name(), renewed.reason(),
                "压缩后他人仍不能抢到仍在租约内的锁");
        log.info("[scenario-锁与压缩] 多次压缩 snapIndex={} 后令牌={} 仍有效，他人竞争 reason={} "
                        + "(判定依据: 锁持有关系不参与日志压缩，租约/栅栏判定不受影响)",
                leader.getSnapshotIndex(), token, renewed.reason());
    }
}
