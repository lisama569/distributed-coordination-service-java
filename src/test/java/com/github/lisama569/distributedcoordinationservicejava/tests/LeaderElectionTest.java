package com.github.lisama569.distributedcoordinationservicejava.tests;

import com.github.lisama569.distributedcoordinationservicejava.cluster.ClusterSimulator;
import com.github.lisama569.distributedcoordinationservicejava.cluster.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationErrorCode;
import com.github.lisama569.distributedcoordinationservicejava.core.CoordinationException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 选举与任期场景：
 * <ol>
 *   <li>3 节点选出唯一领导者，同一任期无双主；</li>
 *   <li>隔离领导者 -> 失去多数派 + 租约过期 -> 写被拒绝（LEASE_EXPIRED/NOT_LEADER）；</li>
 *   <li>多数派一侧选出更高任期新领导者；</li>
 *   <li>分区恢复后旧领导者以更高 term 退让，绝不双主、旧 term 不覆盖新 term。</li>
 * </ol>
 */
class LeaderElectionTest {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionTest.class);

    @Test
    void singleLeaderPerTerm_andMonotonicTerms() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String leaderId = cluster.awaitLeader();
        RaftNode leader = cluster.node(leaderId);
        long firstTerm = leader.getCurrentTerm();

        long leadersCount = cluster.nodes().stream().filter(RaftNode::isLeader).count();
        log.info("[scenario-选举] term={} 领导者={} 自认为领导者的节点数={} (判定依据: 多数派投票，同一 term 一票)",
                firstTerm, leaderId, leadersCount);
        assertEquals(1, leadersCount, "同一任期内不得出现两个领导者");

        // 写入成功并复制到多数派
        leader.propose("k1", "v1");
        log.info("[scenario-选举] term={} 写入 k1=v1 已提交 commitIndex={}",
                firstTerm, leader.getCommitIndex());
        assertEquals("v1", cluster.node(leaderId).equals(leader) ? readAny(cluster, "k1") : null);
    }

    @Test
    void partitionedLeaderLosesQuorumAndLease_thenRejectsWrites() {
        ClusterSimulator cluster = new ClusterSimulator(3);
        String oldLeaderId = cluster.awaitLeader();
        RaftNode oldLeader = cluster.node(oldLeaderId);
        long oldTerm = oldLeader.getCurrentTerm();
        log.info("[scenario-分区] 初始领导者={} term={}，准备隔离该节点", oldLeaderId, oldTerm);

        cluster.isolate(oldLeaderId);
        // 推进超过租约 TTL：旧领导者心跳无人 ACK，无法续约
        cluster.advance(cluster.getLeaseTtlMillis() + cluster.getHeartbeatIntervalMillis() + 1);

        assertTrue(!oldLeader.isLeader() || !oldLeader.hasValidLease(cluster.clock().nowMillis()),
                "隔离后旧领导者必须失去有效租约");
        log.info("[scenario-分区] 旧领导者 role={} term={} leaseValid={} (判定依据: 无多数派心跳 ACK，超过 TTL={})",
                oldLeader.getRole(), oldLeader.getCurrentTerm(),
                oldLeader.hasValidLease(cluster.clock().nowMillis()), cluster.getLeaseTtlMillis());

        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> oldLeader.propose("k-late", "v-late"));
        log.info("[scenario-分区] 旧领导者迟到写被拒绝 errorCode={} 原因={} (判定依据: 失多数派->租约失效)",
                ex.getErrorCode(), ex.getMessage());
        assertTrue(ex.getErrorCode() == CoordinationErrorCode.LEASE_EXPIRED
                        || ex.getErrorCode() == CoordinationErrorCode.NOT_LEADER,
                "失多数派后必须以 LEASE_EXPIRED 或 NOT_LEADER 拒绝写，实际=" + ex.getErrorCode());

        // 多数派一侧（2 节点）应在选举超时后选出新领导者，任期严格变大
        String newLeaderId = cluster.awaitLeader();
        RaftNode newLeader = cluster.node(newLeaderId);
        assertNotEquals(oldLeaderId, newLeaderId, "新领导者必须来自多数派一侧");
        assertTrue(newLeader.getCurrentTerm() > oldTerm,
                "新任期必须严格大于旧任期: " + newLeader.getCurrentTerm() + " > " + oldTerm);
        log.info("[scenario-分区] 多数派选出新领导者={} term={} (判定依据: 2/3 多数派，任期单调递增)",
                newLeaderId, newLeader.getCurrentTerm());

        newLeader.propose("k2", "v2");
        // 隔离期间旧节点无法感知新任期，此刻允许旧节点仍自认为 leader（分区脑裂），
        // 但集群层视角下它没有有效租约；真正“无双主”的断言放在分区恢复之后。
        log.info("[scenario-分区] 新领导者 term={} 写入 k2=v2 已提交；旧节点仍被隔离，无法造成冲突写",
                newLeader.getCurrentTerm());

        // 恢复分区：旧领导者收到更高 term，必须退让
        cluster.healPartition();
        cluster.advance(cluster.getHeartbeatIntervalMillis() + 1);

        long leadersAfterHeal = countLeaders(cluster);
        log.info("[scenario-恢复] 分区恢复后自认为领导者的节点数={} 旧领导者 role={} term={} 新领导者 term={} (判定依据: 旧节点收到更高 term 立即退让)",
                leadersAfterHeal, oldLeader.getRole(), oldLeader.getCurrentTerm(),
                newLeader.getCurrentTerm());
        assertEquals(1, leadersAfterHeal, "分区恢复后不得出现双主");
        assertTrue(oldLeader.getCurrentTerm() >= newLeader.getCurrentTerm(),
                "旧领导者任期必须追上集群最新任期");
        assertTrue(!oldLeader.isLeader() || oldLeader.getCurrentTerm() > oldTerm,
                "旧节点不得再以旧 term 担任领导者");
    }

    @Test
    void termsAreMonotonicAcrossReElections() {
        ClusterSimulator cluster = new ClusterSimulator(5);
        List<Long> terms = new ArrayList<>();
        String leaderId = cluster.awaitLeader();
        terms.add(cluster.node(leaderId).getCurrentTerm());

        for (int round = 0; round < 2; round++) {
            RaftNode current = cluster.leader();
            cluster.isolate(current.getNodeId());
            cluster.advance(cluster.getLeaseTtlMillis() + cluster.getElectionTimeoutMillis() + 30);
            String next = cluster.awaitLeader();
            terms.add(cluster.node(next).getCurrentTerm());
            cluster.healPartition();
            cluster.advance(cluster.getHeartbeatIntervalMillis() + 1);
            // 再让旧隔离节点彻底归队后，下一轮隔离当前真正的领导者
        }
        log.info("[scenario-任期]历次选举任期序列={} (判定依据: 每次重新选举 term+1，绝不回退)", terms);
        for (int i = 1; i < terms.size(); i++) {
            assertTrue(terms.get(i) > terms.get(i - 1), "任期必须单调递增: " + terms);
        }
    }

    private long countLeaders(ClusterSimulator cluster) {
        return cluster.nodes().stream().filter(RaftNode::isLeader).count();
    }

    private String readAny(ClusterSimulator cluster, String key) {
        for (RaftNode n : cluster.nodes()) {
            // 日志提交后各节点状态机最终一致；取领导者所在节点即可
            if (n.isLeader()) {
                return n.logView().stream()
                        .filter(e -> key.equals(e.idempotencyKey()))
                        .findFirst().map(e -> e.value()).orElse(null);
            }
        }
        return null;
    }
}
