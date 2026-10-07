package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteResult;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 滞后节点"快速追赶"场景：追赶必须在少数几轮心跳内完成，
 * 轮数基本不随落后量增长（拒绝时跟随者回传冲突提示，领导者据此把
 * nextIndex 一次跳到位，再整批补齐增量），而不是一轮一轮逐格回退。
 *
 * <ol>
 *   <li>掉线很久 + 期间换过主：落后 120 条，少数几轮追平；</li>
 *   <li>换主后本地带未提交分歧后缀：少数几轮对齐，分歧条目被覆盖、绝不生效；</li>
 *   <li>快照追赶期间集群照常接受并提交新写入，同一次写不被算两遍。</li>
 * </ol>
 */
class FastCatchUpTest {

    private static final Logger log = LoggerFactory.getLogger(FastCatchUpTest.class);

    /** 追平所需心跳轮数上界：与落后量无关的常数（理论 2~3 轮，留足余量）。 */
    private static final int MAX_CATCH_UP_ROUNDS = 5;

    @Test
    void farBehindFollowerAfterLeaderChange_catchesUpWithinBoundedRounds() {
        // 阈值设大，走纯增量路径（不触发压缩），专测增量追赶轮数
        ClusterSimulator cluster = new ClusterSimulator(5, 1000L);
        String l1 = cluster.awaitLeader();
        RaftNode leader1 = cluster.node(l1);

        // 先写 10 条让全员同步
        for (int i = 1; i <= 10; i++) {
            cluster.coordinator(l1).submit(leader1, "sync-" + i, "s" + i);
        }
        cluster.advance(cluster.getHeartbeatIntervalMillis());

        String laggard = cluster.nodes().stream()
                .map(RaftNode::getNodeId).filter(id -> !id.equals(l1)).findFirst().orElseThrow();
        long lagAt = cluster.node(laggard).getLastApplied();
        cluster.isolate(laggard);

        // 落后者掉线期间：写 300 条，随后旧主被隔离、多数派换主，新主再写 20 条
        for (int i = 11; i <= 310; i++) {
            cluster.coordinator(l1).submit(leader1, "lag-" + i, "v" + i);
        }
        cluster.isolate(l1);
        // 推进时钟让旧主租约过期、多数派完成换届
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String l2 = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(l2);
        assertNotEquals(l1, l2, "旧主被隔离后必须换主");
        for (int i = 311; i <= 330; i++) {
            cluster.coordinator(l2).submit(leader2, "lag-" + i, "v" + i);
        }
        long behind = leader2.getLastApplied() - lagAt;
        log.info("[scenario-快速追赶] 落后者={} 掉线于 appliedIndex={}，期间换主({}->{})并写到 {}，"
                + "落后 {} 条（旧实现需 ~{} 轮逐格回退）",
                laggard, lagAt, l1, l2, leader2.getLastApplied(), behind, behind);

        // 恢复连通，等集群稳定后开始计轮
        cluster.healPartition();
        String stableId = cluster.awaitLeader();
        RaftNode stable = cluster.node(stableId);
        int rounds = 0;
        while (cluster.node(laggard).getLastApplied() < stable.getLastApplied()
                && rounds < 200) {
            cluster.advance(cluster.getHeartbeatIntervalMillis());
            rounds++;
        }
        RaftNode caught = cluster.node(laggard);
        log.info("[scenario-快速追赶] {} 落后 {} 条，实际 {} 轮追平 appliedIndex={} "
                + "(判定依据: 冲突提示一次跳位 + 整批补齐，轮数不随落后量增长，上界={})",
                laggard, behind, rounds, caught.getLastApplied(), MAX_CATCH_UP_ROUNDS);
        assertTrue(rounds <= MAX_CATCH_UP_ROUNDS,
                "落后 " + behind + " 条必须在 " + MAX_CATCH_UP_ROUNDS
                        + " 轮内追平，实际=" + rounds);

        // 追平后状态与领导者逐键一致，重新正常参与多数派
        for (int i = 1; i <= 10; i++) {
            assertEquals("s" + i, caught.readState("sync-" + i));
        }
        for (int i = 11; i <= 330; i++) {
            assertEquals("v" + i, caught.readState("lag-" + i),
                    "追平后状态必须与领导者一致: lag-" + i);
        }
        // 追平后的节点能参与多数派：再写一条并提交成功
        WriteResult w = cluster.coordinator(stableId).submit(stable, "post-catchup", "ok");
        assertTrue(w.committed(), "追平后集群必须照常提交新写入");
    }

    @Test
    void followerWithDivergentUncommittedSuffix_catchesUpWithinBoundedRounds() {
        ClusterSimulator cluster = new ClusterSimulator(3, 1000L);
        String l1 = cluster.awaitLeader();
        RaftNode leader1 = cluster.node(l1);
        String n3 = cluster.nodes().stream()
                .map(RaftNode::getNodeId).filter(id -> !id.equals(l1)).findFirst().orElseThrow();
        String f = cluster.nodes().stream()
                .map(RaftNode::getNodeId)
                .filter(id -> !id.equals(l1) && !id.equals(n3)).findFirst().orElseThrow();

        // n3 掉线，L1 与 f 提交 50 条
        cluster.isolate(n3);
        for (int i = 1; i <= 50; i++) {
            cluster.coordinator(l1).submit(leader1, "c-" + i, "v" + i);
        }
        // f 也掉线，L1 失去多数派；租约内（时钟未推进）连续写 5 条 -> 未提交分歧后缀
        cluster.isolate(f);
        for (int i = 51; i <= 55; i++) {
            try {
                leader1.propose("div-" + i, "d" + i);
                fail("失去多数派的领导者不得提交写入");
            } catch (CoordinationException e) {
                assertEquals(CoordinationErrorCode.NO_QUORUM, e.getErrorCode());
            }
        }
        log.info("[scenario-分歧追赶] 旧主={} 本地留有 5 条未提交分歧条目(51..55, term={})",
                l1, leader1.getCurrentTerm());

        // 换主：n3、f 恢复，L1 隔离；日志最新的 f 当选，写 30 条（下标 51..80 覆盖分歧区）
        cluster.healPartition();
        cluster.isolate(l1);
        // 推进时钟让旧主租约过期、多数派完成换届
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String l2 = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(l2);
        assertNotEquals(l1, l2);
        assertTrue(leader2.getCurrentTerm() > leader1.getCurrentTerm(), "新主任期必须更高");
        for (int i = 56; i <= 85; i++) {
            cluster.coordinator(l2).submit(leader2, "c-" + i, "v" + i);
        }

        // 旧主回来：带着 50 提交 + 5 分歧，必须少数几轮对齐，分歧被覆盖
        cluster.healPartition();
        String stableId = cluster.awaitLeader();
        RaftNode stable = cluster.node(stableId);
        int rounds = 0;
        while (cluster.node(l1).getLastApplied() < stable.getLastApplied() && rounds < 200) {
            cluster.advance(cluster.getHeartbeatIntervalMillis());
            rounds++;
        }
        RaftNode back = cluster.node(l1);
        log.info("[scenario-分歧追赶] 旧主={} 带分歧后缀回来，{} 轮对齐 appliedIndex={} term={} "
                + "(判定依据: 任期冲突提示精确跳到分歧段首，上界={})",
                l1, rounds, back.getLastApplied(), back.getCurrentTerm(), MAX_CATCH_UP_ROUNDS);
        assertTrue(rounds <= MAX_CATCH_UP_ROUNDS,
                "带分歧后缀的节点必须少数几轮对齐，实际=" + rounds);

        // 已提交的 80 条一条不少（c-1..c-50 来自旧主，c-56..c-85 来自新主、
        // 其下标 51..80 正好覆盖旧主的分歧区）；未提交的分歧条目绝不生效
        for (int i = 1; i <= 50; i++) {
            assertEquals("v" + i, back.readState("c-" + i),
                    "对齐后状态必须与领导者一致: c-" + i);
        }
        for (int i = 56; i <= 85; i++) {
            assertEquals("v" + i, back.readState("c-" + i),
                    "对齐后状态必须与领导者一致: c-" + i);
        }
        for (int i = 51; i <= 55; i++) {
            assertNull(back.readState("div-" + i),
                    "未提交的分歧条目绝不允许生效: div-" + i);
        }
    }

    @Test
    void writesContinueDuringSnapshotCatchUp_andApplyExactlyOnce() {
        ClusterSimulator cluster = new ClusterSimulator(3, 5L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        String laggard = cluster.nodes().stream()
                .map(RaftNode::getNodeId).filter(id -> !id.equals(leaderId))
                .findFirst().orElseThrow();

        // 落后者掉线期间写 30 条并多轮压缩
        cluster.isolate(laggard);
        for (int i = 1; i <= 30; i++) {
            cluster.coordinator(leaderId).submit(leader, "cw-" + i, "v" + i);
        }
        long snap = leader.getSnapshotIndex();
        assertTrue(snap > 0L, "前置：必须已发生压缩");
        log.info("[scenario-追赶中写入] 落后者={} 错过 30 条，领导者 snapIndex={}",
                laggard, snap);

        // 空盘重置后重新加入：走"存档 + 增量"追赶
        cluster.healPartition();
        cluster.rejoinWithReset(laggard);

        // 追赶尚未完成时就继续写入：每一拍心跳（=一轮追赶）之间都提交新写
        for (int i = 31; i <= 40; i++) {
            WriteResult w = cluster.coordinator(leaderId).submit(leader, "cw-" + i, "v" + i);
            assertTrue(w.committed(), "追赶期间新写入必须照常提交: cw-" + i);
            cluster.advance(cluster.getHeartbeatIntervalMillis());
        }
        log.info("[scenario-追赶中写入] 追赶期间提交 cw-31..cw-40 全部成功，集群未被追赶堵住");

        // 收尾：让落后者彻底追平
        int rounds = 0;
        while (cluster.node(laggard).getLastApplied() < leader.getLastApplied() && rounds < 200) {
            cluster.advance(cluster.getHeartbeatIntervalMillis());
            rounds++;
        }
        RaftNode caught = cluster.node(laggard);
        assertEquals(leader.getLastApplied(), caught.getLastApplied(),
                "存档+增量补齐后必须精确追平（不多不少）");

        // 状态精确一致：40 条一条不少，也没有重复应用造成的错乱
        for (int i = 1; i <= 40; i++) {
            assertEquals("v" + i, caught.readState("cw-" + i),
                    "补齐后状态必须与完整重放一致: cw-" + i);
        }
        // 追赶期间提交的幂等键，在补齐节点上重试：只回放、不二次写入、位点不动
        long appliedBefore = caught.getLastApplied();
        WriteResult replay = cluster.coordinator(laggard).submit(caught, "cw-35", "other");
        assertTrue(replay.dedupHit(), "补齐后旧幂等键必须命中已生效索引");
        assertEquals("v35", replay.value(), "以首次提交值为准");
        assertEquals(appliedBefore, caught.getLastApplied(), "幂等回放不得推进应用位点");
        log.info("[scenario-追赶中写入] {} 追平 appliedIndex={}，幂等键=cw-35 重试 dedupHit={} "
                + "index={} (判定依据: 存档+增量中的已生效索引，同一次写只算一次)",
                laggard, caught.getLastApplied(), replay.dedupHit(), replay.index());
    }
}
