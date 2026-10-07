package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coordination.WriteResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 默认交付语义与追赶时效场景（本轮加固的核心回归）：
 * <ol>
 *   <li><b>默认配置</b>（不显式给任何压缩参数）下持续写入即触发压缩：存档持续推进、
 *       旧日志被真正回收，存档之外保留的增量条数稳定在阈值附近、不再随写入无限上涨；
 *       所有节点（不只领导者）都受压缩约束；</li>
 *   <li>掉线很久 / 期间换过主的节点回来：靠冲突任期提示在极少数几轮内补齐
 *       （轮数不随落后条数增长），追赶期间新写入照常提交，同一条写不会被算两遍；</li>
 *   <li>超出可恢复保留范围（空盘重置以外）：被明确挡回（OUT_OF_RETENTION_WINDOW），
 *       不假装追平；显式重置后以存档+增量在常数轮内追平。</li>
 * </ol>
 */
class DefaultCompactionAndFastCatchUpTest {

    private static final Logger log = LoggerFactory.getLogger(DefaultCompactionAndFastCatchUpTest.class);

    /** 推进一个心跳轮并计数。 */
    private int oneRound(ClusterSimulator cluster) {
        cluster.advance(cluster.getHeartbeatIntervalMillis());
        return 1;
    }

    @Test
    void defaultConfig_sustainedWritesTriggerCompaction_andReclaimsOldLogsBounded() {
        // 关键：使用默认构造 = 默认交付配置，不手工打开任何压缩开关
        ClusterSimulator cluster = new ClusterSimulator(3);
        long threshold = cluster.getCompactionThresholdEntries();
        assertTrue(threshold > 0L, "默认配置必须默认开启压缩，实际 threshold=" + threshold);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        log.info("[scenario-默认压缩] 默认阈值={} 条 (判定依据: 不显式配置即开启)", threshold);

        int total = 260;
        for (int i = 1; i <= total; i++) {
            cluster.coordinator(leaderId).submit(leader, "k" + i, "v" + i);
        }
        cluster.advance(cluster.getHeartbeatIntervalMillis()); // 让跟随者尾部应用/压缩落定

        long snap = leader.getSnapshotIndex();
        int retainedLeader = leader.retainedLogSize();
        log.info("[scenario-默认压缩] 写入 {} 条后 领导者 snapIndex={} 保留增量={} 条 "
                        + "(判定依据: 每 threshold 条已应用增量压一次，旧前缀删除)",
                total, snap, retainedLeader);
        assertTrue(snap >= total - threshold,
                "持续写入后存档必须推进到覆盖大部分历史，实际 snap=" + snap);
        assertTrue(retainedLeader <= threshold + 1L,
                "旧日志必须被真正回收，存档之外增量应稳定在阈值附近，实际保留=" + retainedLeader);

        // 所有跟随者同样被压缩，而不是只有领导者回收
        for (RaftNode n : cluster.nodes()) {
            assertTrue(n.retainedLogSize() <= threshold + 1L,
                    n.getNodeId() + " 保留增量=" + n.retainedLogSize() + " 超出阈值，跟随者没有压缩");
            assertTrue(n.logView().isEmpty() || n.logView().get(0).index() > n.getSnapshotIndex(),
                    "保留日志必须全部位于存档位点之后");
        }

        // 写入继续累加，保留规模仍然有界（"压不下去"的直接反例）
        for (int i = total + 1; i <= total + 120; i++) {
            cluster.coordinator(leaderId).submit(leader, "k" + i, "v" + i);
        }
        cluster.advance(cluster.getHeartbeatIntervalMillis());
        long snap2 = leader.getSnapshotIndex();
        int retained2 = leader.retainedLogSize();
        assertTrue(snap2 > snap, "继续写入后存档位点必须继续前进: " + snap + " -> " + snap2);
        assertTrue(retained2 <= threshold + 1L,
                "继续累加后保留增量仍须有界，实际=" + retained2);
        log.info("[scenario-默认压缩] 再写 120 条: snapIndex {} -> {}，保留增量仍={} 条（有界）",
                snap, snap2, retained2);

        // 存档+增量与从头重放一致；存档内旧键重试只回放、不产生新写
        for (int i = 1; i <= total + 120; i++) {
            assertEquals("v" + i, leader.readState("k" + i), "状态必须与完整重放一致: k" + i);
        }
        WriteResult old = cluster.coordinator(leaderId).submit(leader, "k1", "overwrite");
        assertTrue(old.dedupHit());
        assertEquals(1L, old.index());
        assertEquals("v1", old.value());
        log.info("[scenario-默认压缩] 存档内旧幂等键 k1 重试 dedupHit={} index={} term={} 值={} "
                        + "(判定依据: 幂等索引整体进存档，未确认内容从未进存档)",
                old.dedupHit(), old.index(), old.term(), old.value());
    }

    @Test
    void longLaggingNodeAfterLeaderChange_catchesUpInFewRounds_independentOfLag() {
        // 保留窗口足够大（阈值 1000，全程不压缩），纯粹验证"换主 + 巨落后"下的追赶时效
        ClusterSimulator cluster = new ClusterSimulator(5, 1000L);
        String l1 = cluster.awaitLeader();
        RaftNode leader1 = cluster.node(l1);
        for (int i = 1; i <= 3; i++) {
            cluster.coordinator(l1).submit(leader1, "base-" + i, "b" + i);
        }
        cluster.advance(cluster.getHeartbeatIntervalMillis()); // 让全部节点落定应用

        // 换主：隔离旧主，第二任主在多数派侧上任
        String laggard = "node5";
        cluster.isolate(l1);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String l2 = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(l2);
        assertTrue(leader2.getCurrentTerm() > leader1.getCurrentTerm());

        // 目标掉队节点先随第二任主再落一条，然后彻底失联
        cluster.coordinator(l2).submit(leader2, "base-4", "b4");
        cluster.advance(cluster.getHeartbeatIntervalMillis());
        cluster.isolate(laggard);
        assertEquals(4L, cluster.node(laggard).getLastApplied());

        // 第二任主大量写入
        for (int i = 5; i <= 104; i++) {
            cluster.coordinator(l2).submit(leader2, "bulk-" + i, "v" + i);
        }

        // 第三任主必须在"完全不掌握掉队者真实进度"的状态下上任（nextIndex 初始化为自己的
        // 日志尾，旧实现只会每轮回退一格 -> 轮数随落后长度线性增长）。
        // 先让旧第一任主归队补齐（避免它的低水位日志干扰选举），再让第二任主崩溃，
        // 由健康多数派（node3/node4 + 已补齐的旧主）选出第三任主。
        cluster.healNode(l1);
        for (int hb = 0; hb < 3; hb++) {
            cluster.advance(cluster.getHeartbeatIntervalMillis());
        }
        cluster.crash(l2);
        String l3 = cluster.awaitLeader();
        RaftNode leader3 = cluster.node(l3);
        assertTrue(leader3.getCurrentTerm() > leader2.getCurrentTerm(),
                "第三任主任期必须更高");
        for (int i = 105; i <= 214; i++) {
            cluster.coordinator(l3).submit(leader3, "bulk-" + i, "v" + i);
        }
        cluster.advance(cluster.getHeartbeatIntervalMillis());
        long lag = leader3.getLastApplied() - cluster.node(laggard).getLastApplied();
        assertTrue(lag >= 200L, "前置：掉队差距必须足够大，实际 lag=" + lag);
        log.info("[scenario-快速追赶] 掉队者={} 落后 {} 条，期间换过 2 任主 (term {} -> {} -> {})",
                laggard, lag, leader1.getCurrentTerm(), leader2.getCurrentTerm(), leader3.getCurrentTerm());

        // 回来：冲突任期提示应让 nextIndex 一轮跳到分叉位点，下一轮整段增量补齐
        cluster.healNode(laggard);
        int rounds = 0;
        int writesDuringCatchUp = 0;
        String activeLeaderId = l3;
        for (int t = 0; t < 15; t++) {
            rounds += oneRound(cluster);
            if (cluster.node(laggard).getLastApplied() >= cluster.node(activeLeaderId).getLastApplied()) {
                break;
            }
            // 心跳之后若还有差距，再补一轮"追赶期间的新写入"（不阻塞、且下一轮继续推）
            int seq = 215 + writesDuringCatchUp;
            WriteResult w = cluster.coordinator(activeLeaderId)
                    .submit(cluster.node(activeLeaderId), "during-" + seq, "d" + seq);
            assertTrue(w.committed(), "追赶期间新写入必须照常提交");
            writesDuringCatchUp++;
        }
        RaftNode caught = cluster.node(laggard);
        log.info("[scenario-快速追赶] 补齐轮数={} (落后 {} 条)，追赶期间新提交 {} 条，"
                        + "{} appliedIndex={}/{} (判定依据: AppendResponse 携带冲突任期/位点，一轮跳转)",
                rounds, lag, writesDuringCatchUp, laggard,
                caught.getLastApplied(), cluster.node(l3).getLastApplied());
        assertTrue(rounds <= 3, "追赶必须在极少数几轮内完成且不随落后变长，实际轮数=" + rounds);
        assertEquals(cluster.node(l3).getLastApplied(), caught.getLastApplied(),
                "状态位点必须追平在任领导者");

        // 同一条写不能算两遍：没有任何同位点重复应用；存档之后缺失的增量严格逐条应用一次
        assertEquals(0L, caught.replayApplyCount(),
                "追赶过程不允许任何同位点重复应用，实际 replay=" + caught.replayApplyCount());
        assertEquals(caught.getLastApplied() - caught.getSnapshotIndex(),
                caught.effectiveApplyCount(),
                "追赶开始后缺失的增量必须且只能逐条应用一次，实际 effective="
                        + caught.effectiveApplyCount() + " snap=" + caught.getSnapshotIndex());
        for (int i = 5; i <= 214; i++) {
            assertEquals("v" + i, caught.readState("bulk-" + i), "补齐后状态必须一致: bulk-" + i);
        }
        // 已生效幂等键在补齐节点上重试：回放，不推进位点，不重复应用
        long appliedBeforeRetry = caught.getLastApplied();
        WriteResult replay = cluster.coordinator(laggard).submit(caught, "bulk-50", "again");
        assertTrue(replay.dedupHit());
        assertEquals("v50", replay.value());
        assertEquals(appliedBeforeRetry, caught.getLastApplied());
        assertEquals(appliedBeforeRetry, caught.effectiveApplyCount());
        log.info("[scenario-快速追赶] 幂等键 bulk-50 补齐后重试 dedupHit={} index={}，应用位点/效果数均不变",
                replay.dedupHit(), replay.index());
    }

    @Test
    void resetRejoinBeyondWindow_installsSnapshotAndTail_inConstantRounds() {
        // 掉队超出保留窗口 -> 明确挡回；显式重置后存档+增量仍在常数轮补齐，期间写入不停
        ClusterSimulator cluster = new ClusterSimulator(5, 20L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        String laggard = "node5";
        for (int i = 1; i <= 3; i++) {
            cluster.coordinator(leaderId).submit(leader, "early-" + i, "e" + i);
        }
        cluster.advance(cluster.getHeartbeatIntervalMillis());
        assertEquals(3L, cluster.node(laggard).getLastApplied());

        cluster.crash(laggard);
        for (int i = 4; i <= 43; i++) {
            cluster.coordinator(leaderId).submit(leader, "early-" + i, "e" + i);
        }
        // 失联期间再换一任主
        cluster.isolate(leaderId);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String l2 = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(l2);
        for (int i = 44; i <= 83; i++) {
            cluster.coordinator(l2).submit(leader2, "early-" + i, "e" + i);
        }
        long snap = leader2.getSnapshotIndex();
        assertTrue(snap > 3L, "前置：领导者存档应已覆盖掉队节点全部旧历史，snap=" + snap);
        log.info("[scenario-超窗重置] 掉队者={} 旧位点=3，在任主={} snapIndex={}（远超可恢复窗口）",
                laggard, l2, snap);

        // 唯一合法入口：显式重置为空盘 -> 新一轮心跳直接推存档（代次变化识别）
        cluster.healNode(laggard);
        cluster.rejoinWithReset(laggard);
        int rounds = 0;
        int writesDuringCatchUp = 0;
        for (int t = 0; t < 10; t++) {
            rounds += oneRound(cluster);
            if (cluster.node(laggard).getLastApplied() >= cluster.leader().getLastApplied()) {
                break;
            }
            RaftNode servingLeader = cluster.leader();
            if (servingLeader == null) {
                cluster.advance(cluster.getElectionTimeoutMillis());
                continue;
            }
            String servingId = servingLeader.getNodeId();
            int seq = 84 + writesDuringCatchUp;
            WriteResult w = cluster.coordinator(servingId)
                    .submit(servingLeader, "early-" + seq, "e" + seq);
            assertTrue(w.committed());
            writesDuringCatchUp++;
        }
        RaftNode caught = cluster.node(laggard);
        RaftNode finalLeader = cluster.leader();
        log.info("[scenario-超窗重置] 重置后 {} 轮追平: {} appliedIndex={}/{}，追赶期间新提交 {} 条",
                rounds, laggard, caught.getLastApplied(), finalLeader.getLastApplied(),
                writesDuringCatchUp);
        assertTrue(rounds <= 4, "存档+增量补齐必须为常数轮，实际=" + rounds);
        assertEquals(finalLeader.getLastApplied(), caught.getLastApplied());
        // 存档是整体安装（不逐条重放），存档之后增量按位点严格续接：
        // 不允许任何同位点重复应用；最终内容逐键与在任主一致。
        assertEquals(0L, caught.replayApplyCount(),
                "追赶过程不允许任何同位点重复应用，实际 replay=" + caught.replayApplyCount());
        for (int i = 1; i <= 83; i++) {
            assertEquals("e" + i, caught.readState("early-" + i), "存档+增量状态必须完整: early-" + i);
        }
        assertFalse(finalLeader.blockedCatchUpPeers().containsKey(laggard),
                "追平后不得残留挡回标记");
    }

    @Test
    void divergentTailCompactedAway_isBlockedExplicitly_thenResetRejoins() {
        // 真实分叉：掉队节点在旧任期留下一条"未获多数派确认"的本地尾巴，
        // 多数派在更高任期覆盖了同位点并把分叉点压缩进存档 -> 必须明确挡回。
        // 用 5 节点，保证多数派侧有 3 个日志最新的健康节点，选举不受陈旧节点干扰。
        ClusterSimulator cluster = new ClusterSimulator(5, 10L);
        String l1 = cluster.awaitLeader();
        for (int i = 1; i <= 3; i++) {
            cluster.coordinator(l1).submit(cluster.node(l1), "p-" + i, "p" + i);
        }
        cluster.advance(cluster.getHeartbeatIntervalMillis());

        // 第二任主（node2）任期 2：index=4 获得多数派确认（node3/4/5 中至少两个复制到）
        cluster.isolate(l1);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String l2 = cluster.awaitLeader();
        RaftNode leader2 = cluster.node(l2);
        long term2 = leader2.getCurrentTerm();
        cluster.coordinator(l2).submit(leader2, "p-4", "p4-committed");
        cluster.advance(cluster.getHeartbeatIntervalMillis());
        String diverger = l2; // 第二任主稍后变成掉队者

        // 让它只剩自己：隔离其余所有节点，制造一次必然失败的写，
        // 本地追加 index=5/term=2 但未获多数派确认（幽灵尾巴）
        for (RaftNode n : cluster.nodes()) {
            if (!n.getNodeId().equals(diverger)) {
                cluster.isolate(n.getNodeId());
            }
        }
        boolean rejected = false;
        try {
            cluster.coordinator(diverger).submit(leader2, "p-5-divergent", "ghost");
        } catch (Exception e) {
            rejected = true;
        }
        assertTrue(rejected, "失去多数派的写必须失败，但其本地条目作为未确认尾巴保留");
        assertEquals(5L, cluster.node(diverger).logView().get(4).index());
        assertEquals(term2, cluster.node(diverger).logView().get(4).term());
        // 立即把它崩溃掉：幽灵条目留在磁盘，但内存中的领导者身份/未提交状态全部丢失，
        // 它绝无机会在分区恢复后用这一条凑多数派把幽灵写"洗白"。
        cluster.crash(diverger);
        log.info("[scenario-分叉超窗] 掉队者={} 已崩溃，磁盘保留未确认尾巴 index=5 term={}（多数派从未确认）",
                diverger, term2);

        // 健康三节点一派（node3/4/5，均有已确认的 1..4）恢复连通并选出更高任期新主；
        // 分叉者（已崩溃）与旧主继续隔离，不参与、不干扰选举。
        cluster.healNode("node3");
        cluster.healNode("node4");
        cluster.healNode("node5");
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String l3 = cluster.awaitLeader();
        RaftNode leader3 = cluster.node(l3);
        assertTrue(leader3.getCurrentTerm() > term2);
        for (int i = 5; i <= 30; i++) {
            cluster.coordinator(l3).submit(leader3, "p-" + i, "p" + i);
        }
        cluster.advance(cluster.getHeartbeatIntervalMillis());
        assertTrue(leader3.getSnapshotIndex() >= 5L,
                "前置：分叉位点必须已被多数派压缩进存档，snap=" + leader3.getSnapshotIndex());
        log.info("[scenario-分叉超窗] 多数派在任主={} term={} snapIndex={}，分叉位点 5 已在存档覆盖范围内",
                l3, leader3.getCurrentTerm(), leader3.getSnapshotIndex());

        // 分叉者带盘重启并接回：装存档会抹掉它存档之外的幽灵尾巴 -> 明确挡回
        cluster.restart(diverger);
        cluster.healNode(diverger);
        RaftNode serving = leader3;
        Map<String, String> blocked = Map.of();
        for (int t = 0; t < 80; t++) {
            cluster.advance(1L);
            RaftNode cur = cluster.leader();
            if (cur != null) {
                serving = cur;
                blocked = cur.blockedCatchUpPeers();
                if (blocked.containsKey(diverger)) {
                    break;
                }
            }
        }
        assertTrue(serving.blockedCatchUpPeers().containsKey(diverger),
                "分叉位点已被压缩的掉队节点必须被 OUT_OF_RETENTION_WINDOW 明确挡回，实际="
                        + serving.blockedCatchUpPeers());
        RaftNode staleNode = cluster.node(diverger);
        assertTrue(staleNode.getLastApplied() < serving.getLastApplied(), "不允许假装追平");
        log.warn("[scenario-分叉超窗] {} 被 {} 挡回: {} (判定依据: 未确认尾巴位点已被存档覆盖，"
                        + "覆盖它会丢掉节点自带历史，必须显式重置)",
                diverger, serving.getNodeId(), serving.blockedCatchUpPeers().get(diverger));

        // 与正常竞争失败可区分：挡回原因明确说明要"重置"，错误类别是超窗而非 NOT_LEADER
        assertTrue(serving.blockedCatchUpPeers().get(diverger).contains("重置"));
        assertFalse(staleNode.isLeader());

        // 显式重置 -> 存档+增量真正追平，幽灵值消失
        cluster.rejoinWithReset(diverger);
        for (int t = 0; t < 30; t++) {
            cluster.advance(cluster.getHeartbeatIntervalMillis());
            if (cluster.node(diverger).getLastApplied() >= serving.getLastApplied()) {
                break;
            }
        }
        assertEquals(serving.getLastApplied(), cluster.node(diverger).getLastApplied());
        assertEquals(0L, cluster.node(diverger).replayApplyCount(),
                "重置后安装存档+补增量不允许重复应用");
        assertEquals("p30", cluster.node(diverger).readState("p-30"));
        // 未确认尾巴的幽灵写绝不能出现在最终状态（多数派确认的 p-5 才生效）
        assertFalse("ghost".equals(cluster.node(diverger).readState("p-5-divergent")));
        log.info("[scenario-分叉超窗] 重置后追平 appliedIndex={}，未确认幽灵写被丢弃（只认多数派确认内容）",
                cluster.node(diverger).getLastApplied());
    }

    @Test
    void brandNewEmptyNodeJoiningAfterCompaction_catchesUpInConstantRounds() {
        // 全新空盘节点（非重置，无成员代次变化）在集群已多轮压缩后加入：
        // 乐观探测被拒后必须切换到 InstallSnapshot，而不是在存档位点空转。
        ClusterSimulator cluster = new ClusterSimulator(3, 20L);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        for (int i = 1; i <= 60; i++) {
            cluster.coordinator(leaderId).submit(leader, "n-" + i, "m" + i);
        }
        long snap = leader.getSnapshotIndex();
        assertTrue(snap >= 30L, "前置：加入前集群必须已多轮压缩，snap=" + snap);

        String joiner = "node9";
        cluster.addNode(joiner);
        int rounds = 0;
        for (int t = 0; t < 10; t++) {
            rounds += oneRound(cluster);
            if (cluster.node(joiner).getLastApplied() >= leader.getLastApplied()) {
                break;
            }
        }
        RaftNode joined = cluster.node(joiner);
        log.info("[scenario-新节点加入] 空盘节点 {} 在 snap={} 后加入，{} 轮追平 joinedSnap={} appliedIndex={}/{}，replay={}",
                joiner, snap, rounds, joined.getSnapshotIndex(), joined.getLastApplied(),
                leader.getLastApplied(), joined.replayApplyCount());
        assertTrue(rounds <= 3, "新空盘节点必须常数轮（存档+增量）追平，实际=" + rounds);
        assertEquals(leader.getLastApplied(), joined.getLastApplied());
        assertEquals(0L, joined.replayApplyCount());
        for (int i = 1; i <= 60; i++) {
            assertEquals("m" + i, joined.readState("n-" + i), "新节点状态必须完整: n-" + i);
        }
    }

    @Test
    void sameIdempotencyKey_singleEffect_whetherCompactionBeforeOrAfterRetryAcrossTerms() {
        // 同一幂等键：换主 + 压缩发生在"重试请求前"与"重试请求后"两种时序，
        // 都只能有一次写入效果。
        // 时序 A：先压缩（幂等记录进入存档），再换主，客户端到新主重试
        ClusterSimulator a = new ClusterSimulator(3, 5L);
        String la1 = a.awaitLeader();
        WriteResult firstA = a.coordinator(la1).submit(a.node(la1), "pay-77", "100");
        for (int i = 0; i < 8; i++) {
            a.coordinator(la1).submit(a.node(la1), "fillerA-" + i, "f" + i);
        }
        assertTrue(a.node(la1).getSnapshotIndex() >= firstA.index(), "首次写必须已被压进存档");

        a.isolate(la1);
        a.advance(a.getLeaseTtlMillis() + a.getElectionTimeoutMillis() + 5);
        String la2 = a.awaitLeader();
        long appliedA = a.node(la2).getLastApplied();
        WriteResult retryA = a.coordinator(la2).submit(a.node(la2), "pay-77", "999");
        assertTrue(retryA.dedupHit());
        assertEquals("100", retryA.value());
        assertEquals(firstA.index(), retryA.index());
        assertEquals(appliedA, a.node(la2).getLastApplied(), "压缩后换主重试不得推进应用位点");
        log.info("[scenario-幂等时序A] 先压缩(snap={})后换主(term {})，pay-77 重试 dedupHit={} index={}",
                a.node(la1).getSnapshotIndex(), a.node(la2).getCurrentTerm(),
                retryA.dedupHit(), retryA.index());

        // 时序 B：先重试（压缩尚未发生）确认命中，之后再压缩，再换主再重试，结论仍一致
        ClusterSimulator b = new ClusterSimulator(3, 50L); // 阈值大，前期不压缩
        String lb1 = b.awaitLeader();
        WriteResult firstB = b.coordinator(lb1).submit(b.node(lb1), "pay-88", "200");
        assertEquals(0L, b.node(lb1).getSnapshotIndex(), "时序B前置：首次写后尚未压缩");
        WriteResult retryB1 = b.coordinator(lb1).submit(b.node(lb1), "pay-88", "201");
        assertTrue(retryB1.dedupHit());
        assertEquals("200", retryB1.value());

        b.isolate(lb1);
        b.advance(b.getLeaseTtlMillis() + b.getElectionTimeoutMillis() + 5);
        String lb2 = b.awaitLeader();
        // 新主任期内大量写入触发压缩，把首次写压进存档
        for (int i = 0; i < 60; i++) {
            b.coordinator(lb2).submit(b.node(lb2), "fillerB-" + i, "g" + i);
        }
        assertTrue(b.node(lb2).getSnapshotIndex() >= firstB.index(), "换主后压缩必须覆盖首次写");
        long appliedB = b.node(lb2).getLastApplied();
        WriteResult retryB2 = b.coordinator(lb2).submit(b.node(lb2), "pay-88", "202");
        assertTrue(retryB2.dedupHit(), "压缩发生在请求之后，重试仍必须命中存档内幂等索引");
        assertEquals("200", retryB2.value());
        assertEquals(firstB.index(), retryB2.index());
        assertEquals(appliedB, b.node(lb2).getLastApplied());
        log.info("[scenario-幂等时序B] 先重试命中、后换主(term {})再压缩(snap={})，pay-88 再重试 dedupHit={}",
                b.node(lb2).getCurrentTerm(), b.node(lb2).getSnapshotIndex(), retryB2.dedupHit());
    }

    @Test
    void staleFenceTokenStillRejected_afterCompactionAndCatchUp() {
        // 压缩/追赶不改变栅栏语义：旧令牌的迟到写、租约过期写照样被拒
        ClusterSimulator cluster = new ClusterSimulator(3, 4L);
        String l1 = cluster.awaitLeader();
        var h1 = cluster.lockService().tryAcquire(cluster.node(l1), "res", "owner-A").handle();
        for (int i = 0; i < 8; i++) {
            cluster.coordinator(l1).submit(cluster.node(l1), "w-" + i, "x" + i);
        }
        assertTrue(cluster.node(l1).getSnapshotIndex() > 0L);

        cluster.isolate(l1);
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 5);
        String l2 = cluster.awaitLeader();
        var h2 = cluster.lockService().tryAcquire(cluster.node(l2), "res", "owner-B");
        assertTrue(h2.acquired());
        for (int i = 8; i < 16; i++) {
            cluster.coordinator(l2).submit(cluster.node(l2), "w-" + i, "x" + i);
        }

        var stale = org.junit.jupiter.api.Assertions.assertThrows(
                com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException.class,
                () -> cluster.lockService().validateWriteToken("res", h1.token(),
                        cluster.clock().nowMillis()));
        assertEquals(com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode
                .STALE_FENCE_TOKEN, stale.getErrorCode());
        log.info("[scenario-令牌不回归] 旧主令牌={} 在换主+压缩后迟到写被拒 errorCode={} (判定依据: 栅栏代次已前进)",
                h1.token(), stale.getErrorCode());
        // 新令牌在租约内依然可用
        cluster.lockService().validateWriteToken("res", h2.handle().token(), cluster.clock().nowMillis());
    }
}
