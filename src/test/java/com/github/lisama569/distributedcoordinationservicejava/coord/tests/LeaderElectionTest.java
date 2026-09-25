package com.github.lisama569.distributedcoordinationservicejava.coord.tests;

import com.github.lisama569.distributedcoordinationservicejava.coord.common.CoordinationException;
import com.github.lisama569.distributedcoordinationservicejava.coord.common.RejectReason;
import com.github.lisama569.distributedcoordinationservicejava.coord.raft.RaftNode;
import com.github.lisama569.distributedcoordinationservicejava.coord.sim.Cluster;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 选举与任期测试：多数派当选、任期单调递增、分区下少数派不能选主、
 * 旧领导者租约过期后拒绝写、恢复后不出现双主。
 */
class LeaderElectionTest extends AbstractCoordTest {

    @Test
    void electsSingleLeaderWithMonotonicTerms() {
        printSeparator("选举：多数派当选 + 任期单调");
        Cluster cluster = newThreeNodeCluster();
        String leader1 = awaitLeaderId(cluster, 1000);
        long term1 = cluster.leader().getCurrentTerm();
        assertTrue(term1 >= 1, "初始任期必须为正数");
        System.out.printf("[断言] 选出唯一领导者 %s, 任期=%d%n", leader1, term1);

        // 领导者崩溃，剩余节点选出更高任期的新领导者
        cluster.crash(leader1);
        String leader2;
        long term2;
        RaftNode l2;
        for (long t = 0; t < 1500; t++) {
            cluster.advance(1);
            l2 = cluster.leader();
            if (l2 != null) {
                leader2 = l2.getNodeId();
                term2 = l2.getCurrentTerm();
                System.out.printf("[断言] 旧领导者崩溃后新领导者=%s, 任期=%d (> %d)%n", leader2, term2, term1);
                assertNotEquals(leader1, leader2);
                assertTrue(term2 > term1, "新任期必须单调递增: " + term2 + " > " + term1);
                return;
            }
        }
        fail("旧领导者崩溃后未在限定时间内选出新领导者");
    }

    @Test
    void minorityPartitionCannotElectAndOldLeaderRejectsWritesAfterLeaseExpiry() {
        printSeparator("分区：少数派不能当主，旧领导者租约过期后拒绝写");
        Cluster cluster = newThreeNodeCluster();
        String leader = awaitLeaderId(cluster, 1000);
        long oldTerm = cluster.leader().getCurrentTerm();
        List<String> others = cluster.nodeIds().stream().filter(id -> !id.equals(leader)).toList();

        // 把领导者单独隔离到少数派分区，另外两节点构成多数派
        cluster.split(List.of(leader));

        // 旧领导者在租约内仍可短暂服务，租约过期后必须拒绝写
        cluster.advance(420);
        RaftNode oldLeader = cluster.node(leader);
        CoordinationException ex = assertThrows(CoordinationException.class,
                () -> oldLeader.propose(com.github.lisama569.distributedcoordinationservicejava.coord.raft
                        .ClientRequest.lockAcquire("k", "client-old", "idem-should-fail", 200)));
        System.out.printf("[断言] 被隔离旧领导者 %s 的写被拒绝: 原因=%s%n", leader, ex.getReason());
        assertEquals(RejectReason.LEADER_LEASE_EXPIRED, ex.getReason());
        assertFalse(oldLeader.isLeaderLeaseValid(), "旧领导者租约必须已失效");

        // 多数派分区选出更高任期的新领导者
        String newLeaderId = null;
        long newTerm = -1;
        for (long t = 0; t < 1200; t++) {
            cluster.advance(1);
            for (String id : others) {
                RaftNode n = cluster.node(id);
                if (n.isLeaderLeaseValid()) {
                    newLeaderId = n.getNodeId();
                    newTerm = n.getCurrentTerm();
                }
            }
            if (newLeaderId != null) {
                break;
            }
        }
        assertNotNull(newLeaderId, "多数派分区必须选出新领导者");
        assertTrue(newTerm > oldTerm, "新领导者任期必须更高: " + newTerm + " > " + oldTerm);
        System.out.printf("[断言] 多数派新领导者=%s 任期=%d; 旧领导者=%s 仍停留在任期=%d%n",
                newLeaderId, newTerm, leader, oldTerm);

        // 分区恢复后：旧领导者见到更高任期必须降级，全局恰好一个有效租约领导者
        cluster.heal();
        cluster.advance(200);
        RaftNode current = cluster.leader();
        assertNotNull(current);
        assertEquals(newLeaderId, current.getNodeId(), "恢复后应仍由多数派选出的新领导者领导");
        assertEquals(NodeRole_followerOrLeader(cluster, leader), "FOLLOWER",
                "旧领导者必须已降级为跟随者");
        System.out.printf("[断言] 分区恢复后无旧任期覆盖: 唯一领导者=%s 任期=%d, 旧领导者角色=%s%n",
                current.getNodeId(), current.getCurrentTerm(), cluster.node(leader).getRole());
    }

    private static String NodeRole_followerOrLeader(Cluster cluster, String id) {
        return cluster.node(id).getRole().name();
    }
}
